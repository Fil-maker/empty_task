import csv
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, datetime
from math import isnan
from multiprocessing import Process, Queue
from os import listdir
from os.path import isfile, join
from xml.etree import ElementTree as et
import requests
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import pdfkit
from matplotlib import pyplot as plt
import matplotlib
import numpy as np
from storage import calc_full


# region report

def generate_image(name, period, years_sums, years_sums_cur, years_vacs, years_vacs_cur):
    matplotlib.rc("font", size=8)
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2)
    width = 0.3
    x = np.arange(len(period))
    payment1 = ax1.bar(x - width / 2, years_sums.values(), width, label="средняя з/п")
    payment2 = ax1.bar(x + width / 2, years_sums_cur.values(), width, label=f"з/п {name}")

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
    vac2 = ax2.bar(x + width / 2, years_vacs_cur.values(), width, label=f"Количество вакансий\n{name}")
    ax2.bar_label(vac1, fmt="")
    ax2.bar_label(vac2, fmt="")
    ax2.legend(prop={"size": 6})

    fig.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    plt.savefig("graph.png")


def create_pdf(name, period, years_sums, years_sums_cur, years_length, years_length_cur):
    generate_image(name, period, years_sums, years_sums_cur, years_length, years_length_cur)
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
         "years_stat": years_stat,
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

def calculate_mini(filename, name, q):
    frame = pd.read_csv(filename)
    frame["salary"] = frame.apply(lambda x: func((x.salary_from, x.salary_to, x.salary_currency, x.published_at)),
                                  axis=1)
    am, avg = len(frame), round(frame["salary"].mean(), 2)
    cur = frame[frame["name"].str.lower().str.contains(name.lower())]
    alt_am, alt_avg = len(cur), round(cur["salary"].mean(), 2)
    q.put((am, alt_am, avg, alt_avg))


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


def calc_multi(prof, period):
    procs = {}
    q = Queue()
    sums, vacs, spec_sums, spec_vacs = {}, {}, {}, {}
    with ProcessPoolExecutor(max_workers=32) as executor:
        for year in period:
            proc = Process(target=calculate_mini, args=(f"splits\\split_{year}.csv", prof, q))
            procs[year] = proc
            proc.start()
    for year in period:
        data = q.get()
        sums[year] = data[2]
        vacs[year] = data[0]
        spec_sums[year] = data[3]
        spec_vacs[year] = data[1]
    create_pdf(prof, period, sums, spec_sums, vacs, spec_vacs)


# endregion


if __name__ == '__main__':
    filename, name = 'vacancies/vacancies_dif_currencies.csv', "Программист"

    # years = split_file(filename)
    years = list(range(2003, 2023))
    calc_multi(name, years)
