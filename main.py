import csv
import os


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
                NAME = row.index("name")
                SALARY_FROM = row.index("salary_from")
                SALARY_TO = row.index("salary_to")
                SALARY_CURRENCY = row.index("salary_currency")
                AREA_NAME = row.index("area_name")
                PUBLISHED_AT = row.index("published_at")
                header = row
            else:
                my_row = row.copy()
                if all(my_row):
                    cur_year = int(row[PUBLISHED_AT].split("-")[0])
                    if not mini_files.get(cur_year, False):
                        file = open(f'splits\\split_{cur_year}.csv', 'w', encoding='utf-8', newline='')
                        writer = csv.writer(file, dialect='unix', quoting=csv.QUOTE_MINIMAL)
                        writer.writerow(header)
                        mini_files[cur_year] = writer
                    mini_files[cur_year].writerow(row)

split_file("vacancies_by_year.csv")