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

def calculate_mini(filename, speciality, year, cur_part, q):
    first = False
    am = 0
    avg_s = 0
    alt_am = 0
    alt_avg_s = 0
    mont_curs = {}
    val_ocs = {}
    with open(filename, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if not first:
                first = True
                NAME = row.index("name")
                SALARY_FROM = row.index("salary_from")
                SALARY_TO = row.index("salary_to")
                SALARY_CURRENCY = row.index("salary_currency")
                PUBLISHED_AT = row.index("published_at")
            else:
                if mont_curs.get(row[PUBLISHED_AT][:7], None) is None:
                    mont_curs[row[PUBLISHED_AT][:7]] = create_dic_cur(row[PUBLISHED_AT][:7])
                currency_to_rub = mont_curs[row[PUBLISHED_AT][:7]]
                if row[SALARY_CURRENCY] in currency_to_rub.keys() and \
                        cur_part.get(row[SALARY_CURRENCY], 0) > 5000 \
                        and row[SALARY_CURRENCY] not in [None, '']:
                    # am += 1
                    if val_ocs.get(row[PUBLISHED_AT][:7], None) is None:
                        val_ocs[row[PUBLISHED_AT][:7]] = {}
                    val_ocs[row[PUBLISHED_AT][:7]][row[SALARY_CURRENCY]] = \
                        val_ocs.get(row[PUBLISHED_AT][:7], {}).get(row[SALARY_CURRENCY], 0) + 1
                    # if row[SALARY_FROM] in [None, ''] and row[SALARY_TO] not in [None, '']:
                    #     row[SALARY_FROM] = row[SALARY_TO]
                    # if row[SALARY_TO] in [None, ''] and row[SALARY_FROM] not in [None, '']:
                    #     row[SALARY_TO] = row[SALARY_FROM]
                    # avg_s += (num(row[SALARY_FROM]) + num(row[SALARY_TO])) / 2 * currency_to_rub[row[SALARY_CURRENCY]]
                    # if speciality in row[NAME]:
                    #     alt_am += 1
                    #     alt_avg_s += (num(row[SALARY_FROM]) + num(row[SALARY_TO])) / 2 * currency_to_rub[
                    #         row[SALARY_CURRENCY]]
    # av1, av2 = 0, 0
    # if am != 0:
    #     av1 = int(avg_s // am)
    # if alt_am != 0:
    #     av2 = int(alt_avg_s // alt_am)
    # q.put((year, am, av1, alt_am, av2))
    q.put(val_ocs)


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


def calc_multi(years, mypath, spec, curs):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    procs = {}
    q = Queue()
    sums, vacs, spec_sums, spec_vacs = {}, {}, {}, {}
    with ProcessPoolExecutor(max_workers=32) as executor:
        # for year in years:
        #     ex = executor.submit(calculate_mini, f"splits\\split_{year}.csv", spec, year)
        #     procs.append(ex)
        for year in years:
            proc = Process(target=calculate_mini, args=(f"splits\\split_{year}.csv", spec, year, curs, q))
            procs[year] = proc
            proc.start()
    g_curs = list(filter(lambda i: curs[i] > 5000 and i != '', curs.keys()))
    apps = []
    indexes = []
    for i in range(len(years)):
        # data = procs[i].result()
        data = q.get()
        for key, val in data.items():
            cort = []
            for c in g_curs:
                cort.append(val.get(c, 0) / sum(val.values()))
            apps.append(cort)
            indexes.append(key)
    frame = pd.DataFrame(apps, columns=g_curs, index=indexes)
    frame.index.name = 'date'
    frame.to_csv("result.csv")



# endregion


if __name__ == '__main__':
    # filename = input("Введите название файла:")
    # spec = input("Введите название профессии:")
    filename, spec = "vacancies/vacancies_dif_currencies.csv", "Программист"
    # years, curs = split_file(filename)
    years = range(2003, 2023)
    curs = {'': 1928667, 'USD': 167994, 'RUR': 1830967, 'EUR': 10641, 'KZT': 65291, 'UAH': 25969, 'BYR': 41178,
            'AZN': 607, 'UZS': 2966, 'KGS': 645, 'GEL': 36}
    # print(curs)
    calc_multi(years, 'splits', spec, curs)
