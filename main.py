import csv
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, datetime
from math import isnan
from multiprocessing import Process, Queue
from os import listdir
from os.path import isfile, join
from xml.etree import ElementTree as et

import pandas
import requests
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import pdfkit
from matplotlib import pyplot as plt
import matplotlib
import numpy as np
from storage import calc_full


# region report

def generate_image(name, area_name, period, years_sums, years_sums_cur, years_vacs, years_vacs_cur, ans_cities_sums, cities_partitions):
    matplotlib.rc("font", size=8)
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(nrows=2, ncols=2)
    width = 0.3
    x = np.arange(len(period))
    payment1 = ax1.bar(x - width / 2, years_sums.values(), width, label="средняя з/п")
    payment2 = ax1.bar(x + width / 2, years_sums_cur.values(), width, label=f"з/п {name}\n в городе {area_name}")

    ax1.grid(True, axis="y")
    ax1.set_title("Уровень зарплат по годам")
    ax1.set_xticks(np.arange(len(period)), period, rotation=90)
    ax1.bar_label(payment1, fmt="")
    ax1.bar_label(payment2, fmt="")
    ax1.legend(prop={"size": 6})

    ax2.grid(True, axis="y")
    ax2.set_title("Количество вакансий по годам")
    x = np.arange(len(period))
    ax2.set_xticks(x, period, rotation=90)
    vac1 = ax2.bar(x - width / 2, years_vacs.values(), width, label="Количество вакансий")
    vac2 = ax2.bar(x + width / 2, years_vacs_cur.values(), width, label=f"Количество вакансий\n{name} в {area_name}")
    ax2.bar_label(vac1, fmt="")
    ax2.bar_label(vac2, fmt="")
    ax2.legend(prop={"size": 6})

    ax3.grid(True, axis="x")
    y = np.arange(len(list(ans_cities_sums.keys())))
    ax3.set_yticks(y, map(lambda s: s.replace(" ", "\n").replace("-", "\n"), ans_cities_sums.keys()))
    ax3.invert_yaxis()
    ax3.barh(y, ans_cities_sums.values())
    ax3.set_title("Уровень зарплат по городам")

    ax4.set_title("Доля вакансий по городам")
    other = 1 - sum(cities_partitions.values())
    ax4.pie([other] + list(cities_partitions.values()),
            labels=["Другие"] + list(cities_partitions.keys()), startangle=0)
    fig.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    plt.savefig("graph.png")


def create_pdf(name, area_name, period, years_sums, years_sums_cur, years_length, years_length_cur, ans_cities_sums, cities_partitions):
    generate_image(name, area_name, period, years_sums, years_sums_cur, years_length, years_length_cur, ans_cities_sums, cities_partitions)
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("pdf_template.html")
    pt = os.path.abspath("graph.png")

    years_stat = {}
    for k in years:
        if years_sums.get(k, None) is not None:
            years_stat[k] = [years_sums[k], years_sums_cur[k], years_length[k],
                             years_length_cur[k]]

    pdf_template = template.render(
        {"plot": pt,
         "name": name,
         "area_name": area_name,
         "years_stat": years_stat,
         "cities_sum": ans_cities_sums,
         "cities_part": {key: ((val * 10000) // 1) / 100 for key, val in cities_partitions.items()}
         })
    config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
    pdfkit.from_string(pdf_template, "report.pdf", configuration=config, options={"enable-local-file-access": ""})


# endregion

# region funcs

def num(n):
    return int(float(n))


def create_dic_cur(date):
    resp = requests.get(f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={'01/' + date[5:7] + '/' + date[:4]}")
    tree = et.fromstring(resp.content.decode('windows-1251'))
    out = {"RUR": 1}
    for child in tree:
        out[child[1].text] = float(child[4].text.replace(',', '.')) / int(child[2].text)
    return out


def func(items):
    currency_to_rub = calc_full[f"{items[3][:4]}-{items[3][5:7]}"]
    s_from, s_to, s_cur = items[:3]
    if isnan(s_from) and not isnan(s_to):
        s_from = s_to
    if isnan(s_to) and not isnan(s_from):
        s_to = s_from
    if (isnan(s_to) and isnan(s_from)) or str(s_cur) == 'nan' or s_cur not in currency_to_rub.keys():
        return 0
    return round((s_to + s_from) / 2 * currency_to_rub[s_cur], 2)


# endregion

# region api_parser
def parse(dt_s):
    return dt_s.strftime("%Y-%m-%dT%H:%M:%S")


def api_to_csv():
    st = "2022-12-22T00:00:00"
    init_dt = curr_dt = datetime.strptime(st, "%Y-%m-%dT%H:%M:%S")
    goal_dt = init_dt + timedelta(days=1)
    vacs = []
    ct = 0
    interval_pg = 0
    currency_to_rub = create_dic_cur(st[:4] + '-' + st[5:7])
    while curr_dt < goal_dt:
        interval = 60 * 60 * 24 - interval_pg
        resp = requests.get('https://api.hh.ru/vacancies',
                            params={"specialization": 1, "page": 1, "date_from": parse(curr_dt),
                                    "date_to": parse(curr_dt + timedelta(seconds=interval)), "per_page": 100,
                                    "area": 113}).json()
        while resp["found"] > 2000:
            interval /= 2
            resp = requests.get('https://api.hh.ru/vacancies',
                                params={"specialization": 1, "page": 1, "date_from": parse(curr_dt),
                                        "date_to": parse(curr_dt + timedelta(seconds=interval)), "per_page": 100,
                                        "area": 113}).json()
        for i in range(1, resp["pages"]):
            page = requests.get('https://api.hh.ru/vacancies',
                                params={"specialization": 1, "page": i, "date_from": parse(curr_dt),
                                        "date_to": parse(curr_dt + timedelta(seconds=interval)), "per_page": 100,
                                        "area": 113}).json()
            if page.get("items", None) is None:
                print(resp)
                print(page)
                print(i, resp["pages"], resp["found"])
            for vac in page["items"]:
                if vac.get("salary", None) is None or vac["salary"].get('currency', '') == '':
                    salary = None
                else:
                    salary_from = vac["salary"].get("from", None)
                    salary_to = vac["salary"].get("to", None)
                    salary_cur = vac["salary"].get("currency", None)
                    if salary_from is None and salary_to is not None:
                        salary_from = salary_to
                    if salary_to is None and salary_from is not None:
                        salary_to = salary_from
                    salary = (salary_from + salary_to) / 2 * currency_to_rub[salary_cur]
                vacs.append([vac["name"], salary, vac["area"]["name"], vac["published_at"]])
        curr_dt += timedelta(seconds=interval)
        interval_pg += interval
    frame = pd.DataFrame(vacs, columns=['name', 'salary', 'area_name', 'published_at'])
    frame.set_index('name', inplace=True)
    frame.to_csv("result.csv")


# endregion

# region fast

def calculate_mini(filename, name, area_name, q):
    frame = pd.read_csv(filename)
    # print(len(frame))
    good_frame = (frame
                  .query("salary_to > 0 or salary_from > 0")
                  .assign(
        salary=lambda fr: fr.apply(lambda x: func((x.salary_from, x.salary_to, x.salary_currency, x.published_at)),
                                   axis=1))
                  .assign(published_at=lambda fr: fr["published_at"].apply(lambda x: x[:4]))
                  .drop(['salary_from', 'salary_to', 'salary_currency'], axis=1))
    total_vacancies, avg_salary_total = len(good_frame), round(good_frame["salary"].mean(), 2)
    cur_frame = (good_frame
                 .query(f"area_name == '{area_name}'")
                 .soft_equal("name", name))
    cur_vacancies, avg_salary_cur = len(cur_frame), round(cur_frame["salary"].mean(), 2)
    q.put((total_vacancies, cur_vacancies, avg_salary_total, avg_salary_cur))


def split_file(filename):
    if not os.path.exists(".\\splits"):
        os.mkdir(".\\splits")
    mini_files = {}
    first = False
    header = []
    currencies = {}
    with open(filename, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if not first:
                first = True
                published_at = row.index("published_at")
                currency = row.index("salary_currency")
                header = row
            else:
                # my_row = row.copy()
                # if all(my_row):
                currencies[row[currency]] = currencies.get(row[currency], 0) + 1
                cur_year = int(row[published_at].split("-")[0])
                if not mini_files.get(cur_year, False):
                    file = open(f'splits\\split_{cur_year}.csv', 'w', encoding='utf-8', newline='')
                    writer = csv.writer(file, dialect='unix', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(header)
                    mini_files[cur_year] = writer
                mini_files[cur_year].writerow(row)
    return list(mini_files.keys())


def calc_multi(name, area_name, period):
    procs = {}
    q = Queue()
    sums, vacs, spec_sums, spec_vacs = {}, {}, {}, {}
    with ProcessPoolExecutor(max_workers=32) as executor:
        for year in period:
            proc = Process(target=calculate_mini, args=(f"splits\\split_{year}.csv", name,area_name, q))
            procs[year] = proc
            proc.start()
    for year in period:
        data = q.get()
        sums[year] = data[2]
        vacs[year] = data[0]
        spec_sums[year] = data[3]
        spec_vacs[year] = data[1]
    create_pdf(name, area_name, period, sums, spec_sums, vacs, spec_vacs)

# endregion


def soft_equal(df, key, value):
    return df[df[key].str.contains(value)]


pandas.DataFrame.soft_equal = soft_equal


def test(ser):
    print("Inside test:", ser)


# region single
def full_file_stat(file, name, area_name, period):
    avg_salary_total_year = {}
    avg_salary_cur_year = {}
    vacancy_count_total_year = {}
    vacancy_count_cur_year = {}
    avg_salary_cities = {}
    partition_vacancy_cities = {}

    frame = pd.read_csv(file)
    print("Общее количество вакансий в файле", len(frame))
    good_frame = (frame
                  .query("salary_to > 0 or salary_from > 0")
                  .assign(salary=lambda fr: fr.apply(lambda x: func((x.salary_from, x.salary_to, x.salary_currency, x.published_at)),
                                  axis=1))
                  .assign(published_at=lambda fr: fr["published_at"].apply(lambda x: int(x[:4])))
                  .drop(['salary_from', 'salary_to', 'salary_currency'], axis=1))
    print("Количество 'хороших вакансий'", len(good_frame))

    cities = good_frame.groupby(by="area_name")
    cities.filter(lambda x: x["salary"].count()/len(cities) > 0.01)

    cities_salaries = cities.mean(numeric_only=True).sort_values("salary", ascending=False)
    cities_salaries["salary"] = cities_salaries["salary"].apply(lambda sal: round(sal, 2))
    avg_salary_cities = cities_salaries.loc[:, ["salary"]].iloc[:10, :].to_dict()["salary"]
    print(cities_salaries.loc[:, ["salary"]].iloc[:10, :])

    cities_percent = cities.count()
    cities_percent = (cities_percent
                      .assign(count=lambda fr: fr["name"])
                      .drop(["name", "published_at", "salary"], axis=1)
                      .assign(percent=lambda fr: round(fr["count"]/fr["count"].sum(), 4))
                      .sort_values("percent", ascending=False))
    partition_vacancy_cities = cities_percent.loc[:, ["percent"]].iloc[:10, :].to_dict()["percent"]
    print(cities_percent.loc[:, ["percent"]].iloc[:10, :])

    years = good_frame.groupby("published_at")
    years_avg_salary = years.mean(numeric_only=True)
    avg_salary_total_year = years_avg_salary["salary"].apply(lambda x: round(x, 2)).to_dict()

    years_count_vacancy = years.count()
    vacancy_count_total_year = years_count_vacancy["salary"].to_dict()

    name_area_specific = (good_frame
                          .query(f"area_name == '{area_name}'")
                          .soft_equal("name", name))
    name_area_specific_year_grouped = name_area_specific.groupby("published_at")

    name_area_specific_salary_level = name_area_specific_year_grouped.mean(numeric_only=True).sort_values("published_at")
    avg_salary_cur_year = name_area_specific_salary_level["salary"].apply(lambda x: round(x, 2)).to_dict()
    print(name_area_specific_salary_level)

    name_area_specific_vacancy_number = name_area_specific_year_grouped.count()
    vacancy_count_cur_year = name_area_specific_vacancy_number["salary"].to_dict()
    name_area_specific_vacancy_number = (name_area_specific_vacancy_number
                                         .assign(count=lambda fr: fr["salary"])
                                         .sort_values("published_at"))
    print(name_area_specific_vacancy_number["count"])
    create_pdf(name, area_name, period, avg_salary_total_year, avg_salary_cur_year,
               vacancy_count_total_year, vacancy_count_cur_year,
               avg_salary_cities, partition_vacancy_cities)


# endregion


if __name__ == '__main__':
    filename, name, area_name = 'vacancies/vacancies_dif_currencies.csv', "разработчик", "Санкт-Петербург"

    # years = split_file(filename)
    years = list(range(2003, 2023))
    # calc_multi(name, area_name, years)
    full_file_stat(filename, name, area_name, years)

# There are 4074961 vacancies in vacancies/vacancies_dif_currencies.csv
# Only 2146294 of which are valid
