import io
from unittest import TestCase
import unittest
import unittest.mock
from main import Vacancy, Salary, DataSet, Report


class Tests(TestCase):
    params = {"name": "testName",
              "description": "testDesc",
              "key_skills": ["test1", "test2"],
              "experience_id": "no_experience",
              "premium": False,
              "employer_name": "TestEmp",
              "salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR",
              "area_name": "TestArea",
              "published_at": "12-01-2008 01:32:26:000"}

    def test_salary_type(self):
        self.assertEqual(
            type(Salary({"salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR"})).__name__,
            "Salary")

    def test_salary_to(self):
        self.assertEqual(
            Salary({"salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR"}).salary_to, 7)

    def test_salary_from(self):
        self.assertEqual(
            Salary({"salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR"}).salary_from, 2)

    def test_salary_gross(self):
        self.assertEqual(
            Salary({"salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR"}).salary_gross,
            True)

    def test_salary_currency(self):
        self.assertEqual(
            Salary({"salary_from": 2, "salary_to": 7, "salary_gross": True, "salary_currency": "RUR"}).salary_currency,
            "RUR")

    def test_vacancy_type(self):
        self.assertEqual(type(Vacancy(self.params)).__name__, "Vacancy")

    def test_vacancy_name(self):
        self.assertEqual(Vacancy(self.params).name, "testName")

    def test_vacancy_description(self):
        self.assertEqual(Vacancy(self.params).description, "testDesc")


class FullTest(TestCase):
    dic_trans = {"№": "№",
                 "name": "Название",
                 "description": "Описание",
                 "key_skills": "Навыки",
                 "experience": "Опыт работы",
                 "premium": "Премиум-вакансия",
                 "employer_name": "Компания",
                 "salary": "Оклад",
                 "area_name": "Название региона",
                 "published_at_date": "Дата публикации вакансии"}
    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    def assert_stdout(self, n, expected_output, mock_stdout):
        n("", "", "", self.dic_trans, False, [1, 3])
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    def assert_report(self, n, expected_output, mock_stdout):
        n()
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    def test_assert_majors(self):
        self.assert_stdout(DataSet("vacancies.csv").print_vacancies,
                           """+---+----------------------+----------------------+----------------------+---------------+------------------+----------------------+----------------------+------------------+--------------------------+
| № | Название             | Описание             | Навыки               | Опыт работы   | Премиум-вакансия | Компания             | Оклад                | Название региона | Дата публикации вакансии |
+---+----------------------+----------------------+----------------------+---------------+------------------+----------------------+----------------------+------------------+--------------------------+
| 1 | Руководитель проекта | Обязанности:         | Организаторские      | От 3 до 6 лет | Нет              | ПМЦ Авангард         | 80 000 - 100 000     | Санкт-Петербург  | 17.07.2022               |
|   | по системам связи и  | 1.Участие в          | навыки               |               |                  |                      | (Рубли) (С вычетом   |                  |                          |
|   | информационным       | формировании         | Проведение           |               |                  |                      | налогов)             |                  |                          |
|   | технологиям          | политики и стратегии | презентаций          |               |                  |                      |                      |                  |                          |
|   |                      | развития в           | MS PowerPoint        |               |                  |                      |                      |                  |                          |
|   |                      | направлении связи и  | Информационные       |               |                  |                      |                      |                  |                          |
|   |                      | ИТ(информа...        | технологии           |               |                  |                      |                      |                  |                          |
|   |                      |                      | Аналитическое ...    |               |                  |                      |                      |                  |                          |
+---+----------------------+----------------------+----------------------+---------------+------------------+----------------------+----------------------+------------------+--------------------------+
| 2 | Senior Python        | With over 1,500      | Development          | Более 6 лет   | Нет              | EXNESS Global        | 4 500 - 5 500 (Евро) | Москва           | 05.07.2022               |
|   | Developer (Crypto)   | employees of more    | Python               |               |                  | Limited              | (С вычетом налогов)  |                  |                          |
|   |                      | than 88              | Agile                |               |                  |                      |                      |                  |                          |
|   |                      | nationalities,       | Blockchain           |               |                  |                      |                      |                  |                          |
|   |                      | Exness is the place  | Information          |               |                  |                      |                      |                  |                          |
|   |                      | for global teamwork, | Technology           |               |                  |                      |                      |                  |                          |
|   |                      | in...                |                      |               |                  |                      |                      |                  |                          |
+---+----------------------+----------------------+----------------------+---------------+------------------+----------------------+----------------------+------------------+--------------------------+
""")
    def test_report(self):
        rep = Report("vacancies_by_year.csv", "Программист")
        self.assert_report(rep.print_file,"""Динамика уровня зарплат по годам: {2007: 38916, 2008: 43646, 2009: 42492, 2010: 43846, 2011: 47451, 2012: 48243, 2013: 51510, 2014: 50658, 2015: 52696, 2016: 62675, 2017: 60935, 2018: 58335, 2019: 69467, 2020: 73431, 2021: 82690, 2022: 91795}
Динамика количества вакансий по годам: {2007: 2196, 2008: 17549, 2009: 17709, 2010: 29093, 2011: 36700, 2012: 44153, 2013: 59954, 2014: 66837, 2015: 70039, 2016: 75145, 2017: 82823, 2018: 131701, 2019: 115086, 2020: 102243, 2021: 57623, 2022: 18294}
Динамика уровня зарплат по годам для выбранной профессии: {2007: 43770, 2008: 50412, 2009: 46699, 2010: 50570, 2011: 55770, 2012: 57960, 2013: 58804, 2014: 62384, 2015: 62322, 2016: 66817, 2017: 72460, 2018: 76879, 2019: 85300, 2020: 89791, 2021: 100987, 2022: 116651}
Динамика количества вакансий по годам для выбранной профессии: {2007: 317, 2008: 2460, 2009: 2066, 2010: 3614, 2011: 4422, 2012: 4966, 2013: 5990, 2014: 5492, 2015: 5375, 2016: 7219, 2017: 8105, 2018: 10062, 2019: 9016, 2020: 7113, 2021: 3466, 2022: 1115}
Уровень зарплат по городам (в порядке убывания): {'Москва': 76970, 'Санкт-Петербург': 65286, 'Новосибирск': 62254, 'Екатеринбург': 60962, 'Казань': 52580, 'Краснодар': 51644, 'Челябинск': 51265, 'Самара': 50994, 'Пермь': 48089, 'Нижний Новгород': 47662}
Доля вакансий по городам (в порядке убывания): {'Москва': 0.3246, 'Санкт-Петербург': 0.1197, 'Новосибирск': 0.0271, 'Казань': 0.0237, 'Нижний Новгород': 0.0232, 'Ростов-на-Дону': 0.0209, 'Екатеринбург': 0.0207, 'Краснодар': 0.0185, 'Самара': 0.0143, 'Воронеж': 0.0141}
""")


if __name__ == '__main__':
    unittest.main()
