import json
import time
import urllib.request

from functions import (
    create_database, create_table,
    drop_table, fill_table
)

start_time = time.time()
table_name = "covid_stats"
ecdc_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json"

if __name__ == '__main__':
    print(f"Start Execution: {round(time.time() - start_time, 2)}s")

    create_database()

    print(f"Created Database: {round(time.time() - start_time, 2)}s")

    with urllib.request.urlopen(ecdc_url) as url:
        data = json.loads(url.read().decode())

    print(f"Retrieved data: {round(time.time() - start_time, 2)}s")

    create_table(table_name, data)
    print(f"Created table: {round(time.time() - start_time, 2)}s")

    fill_table(table_name, data)
    print(f"Filled table: {round(time.time() - start_time, 2)}s")
