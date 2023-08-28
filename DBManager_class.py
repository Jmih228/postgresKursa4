import psycopg2

class DBManager:

    def __init__(self):
        conn = psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost")
        cur = conn.cursor()

    def get_companies_and_vacancies_count(self):
        with psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost") as conn:
            with conn.cursor() as cur:

                cur.execute('SELECT company_name, vacancies_count FROM employers')
                rows = cur.fetchall()

                for row in rows:
                    print(row)

    def get_all_vacancies(self):
        with psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost") as conn:
            with conn.cursor() as cur:

                cur.execute('''SELECT company_name, vacancy_name, minimal_salary, vacancy_url
                               FROM vacancies JOIN employers USING(company_id)''')

                rows = cur.fetchall()

                for row in rows:
                    print(row)

    def get_avg_salary(self):
        with psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost") as conn:
            with conn.cursor() as cur:

                cur.execute('SELECT AVG(minimal_salary) FROM vacancies')

                rows = cur.fetchall()

                for row in rows:
                    print(row)

    def get_vacancies_with_higher_salary(self):
        with psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost") as conn:
            with conn.cursor() as cur:

                cur.execute('''SELECT vacancy_name FROM vacancies
                               WHERE minimal_salary > (SELECT AVG(minimal_salary) FROM vacancies)''')

                rows = cur.fetchall()

                for row in rows:
                    print(row)

    def get_vacancies_with_keyword(self, query):
        with psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost") as conn:
            with conn.cursor() as cur:

                cur.execute(f'''SELECT vacancy_name FROM vacancies
                                WHERE LOWER(vacancy_name) LIKE '%{query.lower()}%' ''')

                rows = cur.fetchall()

                for row in rows:
                    print(row)
