import os
import json
import time
import shutil
import psycopg2
import urllib.request
import matplotlib.pyplot as plt

from functions import create_table, fill_table, load_csv


start_time = time.time()
print(f"Start execution: {round(time.time() - start_time, 2)}s")

vax_url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
table_3 = "vax_stats"
json_filename = "vax_data.json"
#public_folder = "C:\\Users\\Public"
#public_path = shutil.copy(os.path.abspath(csv_filename), public_folder)
csv_sql = f'''
    COPY {table_3}
    FROM '{vax_url}' 
    DELIMITER ',' 
    CSV HEADER;
'''

with urllib.request.urlopen(vax_url) as url:
    data =  json.loads(url.read().decode())
    '''try:
        data =  json.loads(url.read().decode())
        with open(json_filename, 'w') as f:
            json.dump(data, f)
    except:
        with open(json_filename) as f:
            print("Using local copy of JSON file")
            data = json.loads(f.read())
'''
print(f"Retrieved data: {round(time.time() - start_time, 2)}s")

create_table(table_3, data)
print(f"Created {table_3} table: {round(time.time() - start_time, 2)}s")

fill_table(table_3, data)
print(f"Filled {table_3} table: {round(time.time() - start_time, 2)}s")