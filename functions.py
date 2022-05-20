import csv
import psycopg2

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

def create_table(table_name, data, csv_data=False):
    if csv_data:
        with open(data) as csv_file:
            reader = csv.reader(csv_file)

            for i, line in enumerate(reader):
                if i == 0:
                    columns = line
                    columns = [f'"{x}"' for x in columns]
                if i == 1:
                    sql_entries = py_to_sql(line)
                    break
    else:
        test_query = data[11685] # 11685 is an index with all columns
        columns = list(test_query.keys())
        sql_entries = py_to_sql(list(test_query.values()))

    table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (\n    '

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

def load_csv(sql):
    params = config()

    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()

    cursor.execute(sql)

    cursor.close()
    connection.close()

# Necessary because the name of all countries comes 
# with a trailing whitespace in the original csv,
# also many countries have different strings for
# their names between the json and the csv files
def new_csv(file_name, new_file):
    with open(file_name) as csv_file:
        reader = csv.reader(csv_file)
        
        with open(new_file, "w", newline='\n') as new_csv:
            
            writer = csv.writer(new_csv)
            for line in reader:
                line[0] = line[0].rstrip()
                if line[0] == "United States":
                    line[0] = "United States Of America"
                elif line[0] == "Bahamas, The":
                    line[0] = "Bahamas"
                elif line[0] == "Central African Rep.":
                    line[0] = "Central African Republic"
                elif line[0] == "Congo, Dem. Rep.":
                    line[0] = "Democratic Republic Of The Congo"
                elif line[0] == "Congo, Repub. of the":
                    line[0] = "Congo"
                elif line[0] == "Cote d'Ivoire":
                    line[0] = "Cote Divoire"
                elif line[0] == "Czech Republic":
                    line[0] = "Czechia"
                elif line[0] == "East Timor":
                    line[0] = "Timor Leste"
                elif line[0] == "Gambia, The":
                    line[0] = "Gambia"
                elif line[0] == "Guinea-Bissau":
                    line[0] = "Guinea Bissau"
                elif line[0] == "Korea, North":
                    line[0] = "North Korea"
                elif line[0] == "Korea, South":
                    line[0] = "South Korea"
                elif line[0] == "Micronesia, Fed. St.":
                    line[0] = "Micronesia (Federated States Of)"
                elif line[0] == "N. Mariana Islands":
                    line[0] = "Northern Mariana Islands"
                elif line[0] == "Saint Kitts & Nevis":
                    line[0] = "Saint Kitts And Nevis"
                elif line[0] == "Saint Vincent and the Grenadines":
                    line[0] = "Saint Vincent And The Grenadines"
                elif line[0] == "Sao Tome & Principe":
                    line[0] = "Sao Tome And Principe"
                elif line[0] == "Trinidad & Tobago":
                    line[0] = "Trinidad And Tobago"
                elif line[0] == "Turks & Caicos Is":
                    line[0] = "Turks And Caicos Islands"
                elif line[0] == "Wallis and Futuna":
                    line[0] == "Wallis And Futuna"
                elif line[0] == "Tanzania":
                    line[0] == "United Republic Of Tanzania"
                writer.writerow(line)

def check_most_recent(table_name, week_str):
    dummy_query = f'''
        SELECT country
        FROM {table_name}
        WHERE year_week='{week_str}'
    '''
    params = config()

    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute(dummy_query)

    while cursor.fetchone() is None:
        year_part, week_part = week_str.split('-')
        week_part = int(week_part)
        week_part -= 1
        week_str = year_part + '-' + str(week_part)

        dummy_query = f'''
            SELECT country
            FROM {table_name}
            WHERE year_week='{week_str}'
        '''

        cursor.execute(dummy_query)
    
    cursor.close()
    connection.close()
    return week_str
