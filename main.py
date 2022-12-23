import csv
import os
from concurrent.futures import ProcessPoolExecutor
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
                if row[SALARY_FROM] in [None, ''] and row[SALARY_TO] not in [None, ''] or SALARY_CURRENCY is None:
                    row[SALARY_FROM] = row[SALARY_TO]
                if row[SALARY_TO] in [None, ''] and row[SALARY_FROM] not in [None, '']:
                    row[SALARY_TO] = row[SALARY_FROM]
                if row[SALARY_FROM] == '' and row[SALARY_TO] == '' or row[SALARY_CURRENCY] == '' or row[SALARY_CURRENCY] not in currency_to_rub.keys():
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
    calc_multi(years, 'splits')
