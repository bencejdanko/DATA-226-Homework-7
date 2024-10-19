from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import datetime

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_session_summary():
    cursor = return_snowflake_conn()
    
    # Create the session_summary table
    cursor.execute("""
    CREATE OR REPLACE TABLE DEV.RAW_DATA.USER_SESSION_DATA AS
    SELECT DISTINCT
        st.SESSIONID,
        usc.USERID,
        usc.CHANNEL,
        st.TS
    FROM 
        DEV.RAW_DATA.SESSION_TIMESTAMP st
    JOIN 
        DEV.RAW_DATA.USER_SESSION_CHANNEL usc
    ON 
        st.SESSIONID = usc.SESSIONID;
    """)
    
    cursor.close()

with DAG(
    dag_id="build_summary",
    start_date=datetime.datetime(2024, 10, 18),
    catchup=False,
    tags=['ETL'],
    schedule_interval='@daily'
) as dag:
    create_session_summary = create_session_summary()

    create_session_summary