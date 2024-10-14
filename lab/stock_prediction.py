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
def train(cur, train_input_table, train_view, forecast_function_name):
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql) 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    fp = Variable.get("forecast_period")
    pred_int = Variable.get("pred_interval")
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => {fp},
            CONFIG_OBJECT => {{'prediction_interval': {pred_int}}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

@task
def fetch_results(cur, table):
    try:
        cur.execute(f"SELECT * FROM {table}")
        df = cur.fetch_pandas_all()
        print(df.to_string())
        print(str(len(df)) + " rows")
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id = 'ml_predict',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '10 0 * * *'
) as dag:
    train_input_table = "lab.raw_data.market_data"
    train_view = "lab.adhoc.market_data_view"
    forecast_table = "lab.adhoc.market_data_forecast"
    forecast_function_name = "lab.analytics.predict_stock_price"
    final_table = "lab.analytics.market_data"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name) >> predict(cur, forecast_function_name, train_input_table, forecast_table, final_table) >> fetch_results(cur, final_table)