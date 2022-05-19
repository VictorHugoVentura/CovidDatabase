import csv
import enum
import json
import time
import psycopg2
import urllib.request

from config import config


# create table command:
'''
CREATE TABLE covid_stats (
    country VARCHAR(255),
    country_code VARCHAR(255),
    continent VARCHAR(255),
    population BIGINT,
    indicator VARCHAR(255),
    weekly_count BIGINT,
    year_week VARCHAR(255),
    rate_14_day VARCHAR(255),
    cumulative_count BIGINT,
    source VARCHAR(255),
    note VARCHAR(255)
);
'''

start_time = time.time()
ecdc_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json"
table_name = "covid_stats"

def create_database():
    params = config()

    sql = f"CREATE DATABASE {params['database']};"

    params['database'] = 'postgres'
    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    except(Exception, psycopg2.errors.DuplicateDatabase) as error:
        print(error)

    cursor.close()
    connection.close()

def create_table(table_name, data):
    '''
    CREATE TABLE IF NOT EXISTS covid_stats (
        country VARCHAR(255),
        country_code VARCHAR(255),
        continent VARCHAR(255),
        population BIGINT,
        indicator VARCHAR(255),
        weekly_count BIGINT,
        year_week VARCHAR(255),
        rate_14_day VARCHAR(255),
        cumulative_count BIGINT,
        source VARCHAR(255),
        note VARCHAR(255)
    );
    '''
    
    test_query = data[11685] # 11685 is an index with all columns
    columns = list(test_query.keys())

    table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (\n    '
    sql_entries = py_to_sql(list(test_query.values()))

    for i, column in enumerate(columns):
        columns[i] = column + ' ' + sql_entries[i]

    table_sql += ', \n    '.join(columns) + "\n);"

    params = config()
    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(table_sql)

    cursor.close()
    connection.close()

def drop_table(table_name):
    params = config()

    sql = f"DROP TABLE IF EXISTS {table_name};"

    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()

    cursor.execute(sql)

    cursor.close()
    connection.close()

def py_to_sql(entries):
    sql_entries = []

    # there are only two types in the json file
    for entry in entries:
        if type(entry) == int:
            sql_entries.append('BIGINT')
        elif type(entry) == str:
            sql_entries.append('VARCHAR(255)')
    
    return sql_entries

def get_insert_str(query, table_name):
    sql_string = f'INSERT INTO {table_name} '

    columns = list(query.keys())
    sql_string += "(" + ', '.join(columns) + ")\nVALUES "

    values = '('
    for value in query.values():

        if type(value) == str:
            value = value.replace("'", "''")
            value = "'" + value + "'"

        values += str(value) + ", "
    values = values[:-2] + ");"

    sql_string += values
    return sql_string

def fill_table(table_name, data):
    params = config()

    try:
        connection = psycopg2.connect(**params)
        connection.autocommit = True

        cursor = connection.cursor()
    except(Exception, psycopg2.Error) as error:
        print(error)
        connection = None
        cursor = None

    if cursor != None:
        for query in data:
            sql = get_insert_str(query, table_name)
            cursor.execute(sql)

    cursor.close()
    connection.close()
