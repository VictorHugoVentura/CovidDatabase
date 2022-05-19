import os
import json
import time
import shutil
import urllib.request

from functions import (
    create_database, create_table,
    fill_table, load_csv
)

start_time = time.time()
table_1 = "covid_stats"
table_2 = "country_stats"
ecdc_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json"
csv_filename = "countries of the world.csv"
public_folder = "C:\\Users\\Public"
public_path = shutil.copy(os.path.abspath(csv_filename), public_folder)

if __name__ == '__main__':
    print(f"Start Execution: {round(time.time() - start_time, 2)}s")

    create_database()

    print(f"Created Database: {round(time.time() - start_time, 2)}s")

    with urllib.request.urlopen(ecdc_url) as url:
        data = json.loads(url.read().decode())

    print(f"Retrieved data: {round(time.time() - start_time, 2)}s")

    create_table(table_1, data)
    print(f"Created {table_1} table: {round(time.time() - start_time, 2)}s")

    fill_table(table_1, data)
    print(f"Filled {table_1} table: {round(time.time() - start_time, 2)}s")

    csv_sql = f'''COPY {table_2}
        FROM '{public_path}' 
        DELIMITER ',' 
        CSV HEADER;'''

    create_table(table_2, csv_filename, csv_data=True)
    print(f"Created {table_2} table: {round(time.time() - start_time, 2)}s")

    load_csv(csv_sql)
    print(f"Filled {table_2} table: {round(time.time() - start_time, 2)}s")
