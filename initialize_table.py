from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage(t1, t2):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {t1} (userId int not NULL, sessionId varchar(32) primary key, channel varchar(32) default 'direct');""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {t2} (sessionId varchar(32) primary key, ts timestamp);""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

@task
def load(s, t1, t2):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE OR REPLACE STAGE {s} url = 's3://s3-geospatial/readonly/' file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');""")
        cur.execute(f"""COPY INTO {t1} FROM @hw7.raw_data.blob_stage/user_session_channel.csv;""")
        cur.execute(f"""COPY INTO {t2} FROM @hw7.raw_data.blob_stage/session_timestamp.csv;""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'create_table',
    start_date = datetime(2024,10,20),
    catchup=False,
    tags=['ELT'],
    schedule = '30 2 * * *'
) as dag:
    table_1 = "hw7.raw_data.user_session_channel"
    table_2 = "hw7.raw_data.session_timestamp"
    stage = "hw7.raw_data.blob_stage"

    set_stage(table_1, table_2) >> load(stage, table_1, table_2)

