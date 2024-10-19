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
    CREATE OR REPLACE TABLE analytics.session_summary AS
    SELECT 
        us.user_id,
        st.session_id,
        st.timestamp,
        ROW_NUMBER() OVER (PARTITION BY us.user_id, st.session_id ORDER BY st.timestamp) as row_num
    FROM 
        dev.raw_data.user_session_channel us
    JOIN 
        dev.raw_data.session_timestamp st
    ON 
        us.session_id = st.session_id
    QUALIFY row_num = 1;
    """)
    
    cursor.close()

with DAG(
    dag_id="create_session_summary",
    start_date=datetime.datetime(2024, 10, 18),
    catchup=False,
    tags=['ETL'],
    schedule_interval='@daily'
) as dag:
    create_session_summary = create_session_summary()

    create_session_summary