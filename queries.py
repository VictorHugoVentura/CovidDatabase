import time
import datetime
import psycopg2
import pandas as pd

from config import config
from functions import check_most_recent


start_time = time.time()
print(f"Start execution: {round(time.time() - start_time, 2)}s")
view_name = "recent_cases"
table_1 = "covid_stats"
table_2 = "country_stats"

now = datetime.datetime.now()
current_week = datetime.date(now.year, now.month, now.day).isocalendar()
current_week_str = str(current_week[0]) + '-' + str(current_week[1])
most_recent_week = check_most_recent("covid_stats", current_week_str)

desired_date = "31/07/2020"
desired_week = datetime.date(2020, 7, 31).isocalendar()
desired_week_str = str(desired_week[0]) + '-' + str(desired_week[1])

sql_view = f'''CREATE OR REPLACE VIEW {view_name} AS
    SELECT {table_2}.*, cumulative_count, rate_14_day, year_week
    FROM {table_1}, {table_2}
    WHERE year_week='{most_recent_week}' AND
    indicator='cases' AND
    {table_1}.country={table_2}."Country"'''

params = config()

connection = psycopg2.connect(**params)
connection.autocommit = True

cursor = connection.cursor()

cursor.execute(sql_view)
print(f"Created view: {round(time.time() - start_time, 2)}s")

print("Query 1")
print("What is the country with the highest number of Covid-19 cases per 100 000 inhabitants at 31/07/2020?\n")

sql_1 = '''
SELECT country, cast(cumulative_count as double precision) * 100000 / population as case_rate
FROM covid_stats
WHERE year_week='2020-31' AND indicator='cases'
ORDER BY case_rate DESC
LIMIT 1;
'''

cursor.execute(sql_1)
result_1 = cursor.fetchone()

print("Answer 1")
print(f'''The country with the highest case rate per 100000 in {desired_date} is {result_1[0]} with a case rate of {round(result_1[1], 2)} per 100000.\n''')
print(f"Answered in {round(time.time() - start_time, 2)}s\n")

print("Query 2")
print("What are the 10 countries with the lowest number of Covid-19 cases per 100 000 inhabitants at 31/07/2020?\n")
sql_2 = '''
SELECT country, cast(cumulative_count as double precision) * 100000 / population as case_rate
FROM covid_stats
WHERE year_week='2020-31' AND indicator='cases'
ORDER BY case_rate
LIMIT 10;
'''

cursor.execute(sql_2)
result_2 = cursor.fetchall()
result_2 = [x[0] for x in result_2]

print("Answer 2")
print(f"The 10 countries with the lowest Covid-19 case rate per 100000 are {', '.join(result_2)}.\n")
print(f"Answered in {round(time.time() - start_time, 2)}s\n")


print("Query 3")
print("What are the top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)? (no date mentioned, assumed most recent)\n")

sql_3 = f'''
SELECT "Country", cumulative_count, "GDP ($ per capita)"
FROM {view_name}
WHERE "GDP ($ per capita)" IS NOT NULL
ORDER BY cast("GDP ($ per capita)" as int) DESC, cumulative_count
LIMIT 20;
'''

cursor.execute(sql_3)
result_3 = cursor.fetchall()
result_3 = sorted(result_3, key=lambda tup: tup[1], reverse=True)[:10]
result_3 = [x[0] for x in result_3]

print("Answer 3")
print(f'''The 10 countries with the highest number of cases among the top 20 richest countries by GDP per capita are {', '.join(result_3)}.\n''')
print(f"Answered in {round(time.time() - start_time, 2)}s\n")

print("query 4")
print("List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020.\n")

# The view created in exercise 3 only has the most recent week in the json file

view_4_name = "cases_4"

view_4 = f'''CREATE OR REPLACE VIEW {view_4_name} AS
    SELECT {table_2}.*, cumulative_count, rate_14_day, year_week
    FROM {table_1}, {table_2}
    WHERE year_week='{desired_week_str}' AND
    indicator='cases' AND
    {table_1}.country={table_2}."Country"
'''

cursor.execute(view_4)
print(f"Created second view: {round(time.time() - start_time, 2)}s\n")

sql_4 = f'''SELECT "Region", to_char(SUM(cumulative_count) * 1000000/SUM(CAST("Population" AS INT)), 'FM999999999.00') AS covid_density,
to_char(SUM(CAST("Population" AS INT))/SUM(CAST("Area (sq. mi.)" AS DOUBLE PRECISION)), 'FM999999999.00') AS population_density
FROM {view_4_name}
WHERE year_week='{desired_week_str}'
GROUP BY "Region", year_week
ORDER BY covid_density desc;'''

cursor.execute(sql_4)
table = cursor.fetchall()

df = pd.DataFrame(table, columns = ['Regions','Covid density','Pop density'])
print(df)

cursor.close()
connection.close()
