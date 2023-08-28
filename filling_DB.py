import requests
import json
import psycopg2

employers_id_hhru = ['1075575', '3833791', '2277365', '5094353', '5902137', '681319', '815055', '9977674', '1146142', '736233']

conn = psycopg2.connect(dbname="postgres", user="postgres", password="hghghg777", host="localhost")
cur = conn.cursor()

conn.autocommit = True
cur.execute('CREATE DATABASE hhru_project')

conn = psycopg2.connect(dbname="hhru_project", user="postgres", password="hghghg777", host="localhost")
cur = conn.cursor()

cur.execute('''CREATE TABLE employers (
               company_id serial NOT NULL,
               company_name character varying(40) NOT NULL,
               vacancies_count int)''')

cur.execute('''CREATE TABLE vacancies (
               vacancy_id serial NOT NULL,
               vacancy_name character varying(100) NOT NULL,
               minimal_salary int,
               vacancy_url character varying(50) NOT NULL,
               company_id smallint NOT NULL)''')

for i in range(len(employers_id_hhru)):
    request = json.loads(requests.get(f'https://api.hh.ru/employers/{employers_id_hhru[i]}').text)

    cur.execute('INSERT INTO employers VALUES (%s, %s, %s)', (i + 1,
                                                              request['name'],
                                                              request['open_vacancies']))

    for j in range(request['open_vacancies']):
        vacancies_req = json.loads(requests.get(f'https://api.hh.ru/vacancies?employer_id={employers_id_hhru[i]}').text)['items'][j]

        cur.execute('INSERT INTO vacancies VALUES(%s, %s, %s, %s, %s)', (j + 1,
                                                                         vacancies_req['name'],
                                                                         vacancies_req['salary']['from'] if vacancies_req['salary'] is not None else None,
                                                                         vacancies_req['url'],
                                                                         i + 1))

cur.close()
conn.close()
