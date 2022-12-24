import csv
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, datetime
from multiprocessing import Process, Queue
from os import listdir
from os.path import isfile, join
from xml.etree import ElementTree as et
import requests
import pandas as pd
import numpy as np


def num(n):
    return int(float(n))


def create_dic_cur(date):
    resp = requests.get(f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={'01/' + date[5:] + '/' + date[:4]}")
    tree = et.fromstring(resp.content.decode('windows-1251'))
    out = {"RUR": 1}
    for child in tree:
        out[child[1].text] = float(child[4].text.replace(',', '.')) / int(child[2].text)
    return out


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


# region fast

def calculate_mini(filename, q):
    first = False
    mont_curs = {}
    vacs = []
    with open(filename, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if not first:
                first = True
                NAME = row.index("name")
                SALARY_FROM = row.index("salary_from")
                SALARY_TO = row.index("salary_to")
                SALARY_CURRENCY = row.index("salary_currency")
                AREA_NAME = row.index("area_name")
                PUBLISHED_AT = row.index("published_at")
            else:
                if mont_curs.get(row[PUBLISHED_AT][:7], None) is None:
                    mont_curs[row[PUBLISHED_AT][:7]] = create_dic_cur(row[PUBLISHED_AT][:7])
                currency_to_rub = mont_curs[row[PUBLISHED_AT][:7]]
                if row[SALARY_FROM] in [None, ''] and row[SALARY_TO] not in [None, '']:
                    row[SALARY_FROM] = row[SALARY_TO]
                if row[SALARY_TO] in [None, ''] and row[SALARY_FROM] not in [None, '']:
                    row[SALARY_TO] = row[SALARY_FROM]
                if row[SALARY_FROM] == '' and row[SALARY_TO] == '' or row[SALARY_CURRENCY] == '' or row[
                    SALARY_CURRENCY] not in currency_to_rub.keys():
                    salary = None
                else:

                    salary = (float(row[SALARY_FROM]) + float(row[SALARY_TO])) * currency_to_rub[row[SALARY_CURRENCY]]
                vacs.append([row[NAME], salary, row[AREA_NAME], row[PUBLISHED_AT]])
    q.put(vacs)


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
    return list(mini_files.keys()), currencies


def calc_multi(years, mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    procs = {}
    q = Queue()
    sums, vacs, spec_sums, spec_vacs = {}, {}, {}, {}
    with ProcessPoolExecutor(max_workers=32) as executor:
        # for year in years:
        #     ex = executor.submit(calculate_mini, f"splits\\split_{year}.csv", spec, year)
        #     procs.append(ex)
        for year in years:
            proc = Process(target=calculate_mini, args=(f"splits\\split_{year}.csv", q))
            procs[year] = proc
            proc.start()

    apps = []
    indexes = []
    for i in range(len(years)):
        # data = procs[i].result()
        data = q.get()
        apps = apps + data
    frame = pd.DataFrame(apps, columns=['name', 'salary', 'area_name', 'published_at'])
    frame.set_index('name', inplace=True)
    frame.to_csv("result.csv")


# endregion


if __name__ == '__main__':
    # filename = input("Введите название файла:")
    # spec = input("Введите название профессии:")
    filename, spec = "vacancies/vacancies_dif_currencies.csv", "Программист"
    # years, curs = split_file(filename)
    years = range(2003, 2023)
    # curs = {'': 1928667, 'USD': 167994, 'RUR': 1830967, 'EUR': 10641, 'KZT': 65291, 'UAH': 25969, 'BYR': 41178,
    #         'AZN': 607, 'UZS': 2966, 'KGS': 645, 'GEL': 36}
    # print(curs)
    # calc_multi(years, 'splits')
    api_to_csv()
