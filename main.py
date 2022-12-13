import csv
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process, Queue
import cProfile
from os import listdir
from os.path import isfile, join

currency_to_rub = {
    "AZN": 35.68,
    "BYR": 23.91,
    "EUR": 59.90,
    "GEL": 21.74,
    "KGS": 0.76,
    "KZT": 0.13,
    "RUR": 1,
    "UAH": 1.64,
    "USD": 60.66,
    "UZS": 0.0055,
}


def num(n):
    return int(float(n))


# region fast

def calculate_mini(filename, speciality, year):
    first = False
    am = 0
    avg_s = 0
    alt_am = 0
    alt_avg_s = 0
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
                am += 1
                avg_s += (num(row[SALARY_FROM]) + num(row[SALARY_TO])) / 2 * currency_to_rub[row[SALARY_CURRENCY]]
                if speciality in row[NAME]:
                    alt_am += 1
                    alt_avg_s += (num(row[SALARY_FROM]) + num(row[SALARY_TO])) / 2 * currency_to_rub[
                        row[SALARY_CURRENCY]]
    return ((year, am, int(avg_s // am), alt_am, int(alt_avg_s // alt_am)))


def split_file(filename):
    if not os.path.exists(".\\splits"):
        os.mkdir(".\\splits")
    mini_files = {}
    first = False
    header = []
    with open(filename, encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if not first:
                first = True
                published_at = row.index("published_at")
                header = row
            else:
                my_row = row.copy()
                if all(my_row):
                    cur_year = int(row[published_at].split("-")[0])
                    if not mini_files.get(cur_year, False):
                        file = open(f'splits\\split_{cur_year}.csv', 'w', encoding='utf-8', newline='')
                        writer = csv.writer(file, dialect='unix', quoting=csv.QUOTE_MINIMAL)
                        writer.writerow(header)
                        mini_files[cur_year] = writer
                    mini_files[cur_year].writerow(row)
    return list(mini_files.keys())


def calc_multi(years, mypath, spec):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    procs = []
    q = Queue()
    sums, vacs, spec_sums, spec_vacs = {}, {}, {}, {}
    with ProcessPoolExecutor(max_workers=16) as executor:
        for year in years:
            ex = executor.submit(calculate_mini, f"splits\\split_{year}.csv", spec, year)
            procs.append(ex)
        # for year in years:
        #     proc = Process(target=calculate_mini, args=(f"splits\\split_{year}.csv", spec, year, q))
        #     procs[year] = proc
        #     proc.start()
    for i in range(len(years)):
        data = procs[i].result()
        # data = q.get()
        sums[data[0]] = data[1]
        vacs[data[0]] = data[2]
        spec_sums[data[0]] = data[3]
        spec_vacs[data[0]] = data[4]
    print(f"Динамика уровня зарплат по годам: {dict(sorted(sums.items(), key=lambda x: x[0]))}")
    print(f"Динамика количества вакансий по годам: {dict(sorted(vacs.items(), key=lambda x: x[0]))}")
    print(f"Динамика уровня зарплат по годам для выбранной профессии: "
          f"{dict(sorted(spec_vacs.items(), key=lambda x: x[0]))}")
    print(f"Динамика количества вакансий по годам для выбранной профессии: "
          f"{dict(sorted(spec_sums.items(), key=lambda x: x[0]))}")


# endregion

# region long

def read_file(filename, name):
    global vacancies_length
    first = False
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
                if all(row.copy()):
                    cur_year = int(row[PUBLISHED_AT].split("-")[0])
                    cur_salary = (int(float(row[SALARY_TO])) + int(float(row[SALARY_FROM]))) * currency_to_rub[
                        row[SALARY_CURRENCY]] // 2
                    cur_name = row[NAME]
                    cur_city = row[AREA_NAME]
                    years_sums[cur_year] = years_sums.get(cur_year, 0) + cur_salary
                    years_length[cur_year] = years_length.get(cur_year, 0) + 1
                    if name in cur_name:
                        years_sums_cur[cur_year] = years_sums_cur.get(cur_year, 0) + cur_salary
                        years_length_cur[cur_year] = years_length_cur.get(cur_year, 0) + 1
                    if cur_city not in cities:
                        cities.append(cur_city)
                    cities_sums[cur_city] = cities_sums.get(cur_city, 0) + cur_salary
                    cities_length[cur_city] = cities_length.get(cur_city, 0) + 1
                    vacancies_length += 1


years = [i for i in range(2007, 2023)]
years_sums = {}
years_length = {}
years_sums_cur = {}
years_length_cur = {}
cities = []
cities_sums = {}
cities_length = {}
vacancies_length = 0


def calc_long(filename, name):
    read_file(filename, name)

    for i in years:
        if years_sums.get(i, None):
            years_sums[i] = int(years_sums[i] // years_length[i])
        if years_sums_cur.get(i, None):
            years_sums_cur[i] = int(years_sums_cur[i] // years_length_cur[i])

    for i in cities:
        cities_sums[i] = int(cities_sums[i] // cities_length[i])
    interesting_cities = [city for city in cities if cities_length[city] >= vacancies_length // 100]
    ans_cities_sums = {key: cities_sums[key] for key in
                       sorted(interesting_cities, key=lambda x: cities_sums[x], reverse=True)[:10]}
    cities_partitions = {key: float("{:.4f}".format(cities_length[key] / vacancies_length)) for key in
                         sorted(interesting_cities, key=lambda x: cities_length[x] / vacancies_length, reverse=True)[
                         :10]}
    print("Динамика уровня зарплат по годам:", years_sums)
    print("Динамика количества вакансий по годам:", years_length)
    if not len(years_sums_cur):
        years_sums_cur[2022] = 0
    print("Динамика уровня зарплат по годам для выбранной профессии:", years_sums_cur)
    if not len(years_length_cur):
        years_length_cur[2022] = 0
    print("Динамика количества вакансий по годам для выбранной профессии:", years_length_cur)
    print("Уровень зарплат по городам (в порядке убывания):", ans_cities_sums)
    print("Доля вакансий по городам (в порядке убывания):", cities_partitions)


# endregion

if __name__ == '__main__':
    # filename = input("Введите название файла:")
    # spec = input("Введите название профессии:")
    filename, spec = "vacancies_by_year.csv", "Программист"
    # filename = input("Введите название файла: ")
    # name = input("Введите название профессии: ")
    # cProfile.run("calc_long(filename, spec)")
    years = split_file(filename)
    cProfile.run("calc_multi(years, 'splits', spec)")
