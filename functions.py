import csv
import psycopg2

from config import config

country_names_dict = {
    "United States": "United States Of America",
    "Bahamas, The": "Bahamas",
    "Central African Rep.": "Central African Republic",
    "Congo, Dem. Rep.": "Democratic Republic Of The Congo",
    "Congo, Repub. of the": "Congo",
    "Cote d'Ivoire": "Cote Divoire",
    "Czech Republic": "Czechia",
    "East Timor": "Timor Leste",
    "Gambia, The": "Gambia",
    "Guinea-Bissau": "Guinea Bissau",
    "Korea, North": "North Korea",
    "Korea, South": "South Korea",
    "Micronesia, Fed. St.": "Micronesia (Federated States Of)",
    "N. Mariana Islands": "Northern Mariana Islands",
    "Saint Kitts & Nevis": "Saint Kitts And Nevis",
    "Saint Vincent and the Grenadines": "Saint Vincent And The Grenadines",
    "Sao Tome & Principe": "Sao Tome And Principe",
    "Trinidad & Tobago": "Trinidad And Tobago",
    "Turks & Caicos Is": "Turks And Caicos Islands",
    "Wallis and Futuna": "Wallis And Futuna",
    "Tanzania": "United Republic Of Tanzania"
}

regions_dict = {
    "ASIA (EX. NEAR EAST)": "ASIA (EXCEPT NEAR EAST)",
    "LATIN AMER. & CARIB": "LATIN AMERICA AND CARIBBEAN",
    "C.W. OF IND. STATES": "COMMONWEALTH OF INDEPENDENT STATES"
}


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
                if line[0] in country_names_dict.keys():
                    line[0] = country_names_dict[line[0]]
                
                line[1] = line[1].strip()
                if line[1] in regions_dict.keys():
                    line[1] = regions_dict[line[1]]
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
