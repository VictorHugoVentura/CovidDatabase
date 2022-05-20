import datetime
import psycopg2

from config import config
from functions import check_most_recent


view_name = "recent_cases"
table_1 = "covid_stats"
table_2 = "country_stats"

now = datetime.datetime.now()
current_week = datetime.date(now.year, now.month, now.day).isocalendar()
current_week_str = str(current_week[0]) + '-' + str(current_week[1])
most_recent_week = check_most_recent("covid_stats", current_week_str)

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


desired_date = "31/07/2020"
desired_week = datetime.date(2020, 7, 31).isocalendar()
desired_week_str = str(desired_week[0]) + '-' + str(desired_week[1])

# query 1
# What is the country with the highest number of Covid-19 cases per 100 000 inhabitants at 31/07/2020?
sql_1 = '''
SELECT country, cast(cumulative_count as double precision) * 100000 / population as case_rate
FROM covid_stats
WHERE year_week='2020-31' AND indicator='cases'
ORDER BY case_rate DESC
LIMIT 1;
'''

cursor.execute(sql_1)
result_1 = cursor.fetchone()

print(f'''The country with the highest case rate per 100000 in
{desired_date} is {result_1[0]} with a case rate of {round(result_1[1], 2)} per 100000.\n''')

# query 2
# What are the 10 countries with the lowest number of Covid-19 cases per 100 000 inhabitants at 31/07/2020?
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

print(f"The 10 countries with the lowest Covid-19 case rate per 100000 are {', '.join(result_2)}.\n")


# query 3
# What is the top 10 countries with the highest number of cases among the top 20 richest 
# countries (by GDP per capita)? (no date mentioned, assumed most recent)

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

print(f'''The 10 countries with the highest number of cases among the top 20 richest countries by GDP per capita are
{', '.join(result_3)}.\n''')

cursor.close()
connection.close()
