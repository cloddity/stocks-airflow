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
    results = []
    date = data["Time Series (Daily)"]
    for d in date:
        stock_info = date[d]
        stock_info['date'] = d + ' 13:00:00.000'
        results.append(stock_info)
    return results

@task
def load(con, records, stock, target_table):
    try:
        con.execute("BEGIN;")
        cur.execute("CREATE DATABASE IF NOT EXISTS lab")
        cur.execute("CREATE SCHEMA IF NOT EXISTS adhoc")
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
        con.execute(f"CREATE OR REPLACE TABLE {target_table} (DATE TIMESTAMP_NTZ PRIMARY KEY, SYMBOL VARCHAR(6), OPEN FLOAT, CLOSE FLOAT, LOW FLOAT, HIGH FLOAT, VOLUME FLOAT);")
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
        raise

with DAG(
    dag_id = 'snowflake',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ETL'],
    schedule = '0 0 * * *'
) as dag:
    url = Variable.get("vantage_api_key")
    sym = Variable.get("symbol")
    cur = return_snowflake_conn()
    target_table = "lab.raw_data.market_data"

    data = extract(url, sym)
    stock_info = transform(data)
    load(cur, stock_info, sym, target_table)