from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(vantage_api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url)
    return r.json()

@task
def transform(data):
    num = 0
    results = []
    for d in data["Time Series (Daily)"]:
        stock_info = data["Time Series (Daily)"][d]
        stock_info['date'] = d
        results.append(stock_info)
        num += 1
        if num == 90:
            break
    return results

@task
def load(con, records, stock):
    target_table = "STOCKS_TB"
    try:
        con.execute("BEGIN;")
        con.execute("CREATE DATABASE IF NOT EXISTS STOCKS_DB")
        con.execute("USE DATABASE STOCKS_DB")
        con.execute("CREATE SCHEMA IF NOT EXISTS RAW_DATA")
        con.execute("USE SCHEMA RAW_DATA")
        con.execute(f"CREATE OR REPLACE TABLE {target_table} (DATE DATE PRIMARY KEY, SYMBOL VARCHAR(6), OPEN FLOAT, CLOSE FLOAT, LOW FLOAT, HIGH FLOAT, VOLUME FLOAT);")
        for r in records:
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            date = r["date"]
            symbol = stock

            sql = f"INSERT INTO {target_table} (date, symbol, open, close, low, high, volume) VALUES ('{date}', '{symbol}', '{open}', '{close}', '{low}', '{high}', '{volume}')"
            con.execute(sql)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'snowflake',
    start_date = datetime(2024,10,9),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    url = Variable.get("vantage_api_key")
    cur = return_snowflake_conn()
    stock = "NVDA"

    data = extract(url, "NVDA")
    lines = transform(data)
    load(cur, lines, stock)