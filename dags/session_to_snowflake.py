from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import datetime

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage():
    cursor = return_snowflake_conn()
    cursor.execute("CREATE OR REPLACE STAGE dev.raw_data.blob_stage url = 's3://s3-geospatial/readonly/' file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '\"');")
    cursor.close()

@task
def load():
    cursor = return_snowflake_conn()
    cursor.execute("COPY INTO dev.raw_data.user_session_channel FROM @dev.raw_data.blob_stage/user_session_channel.csv;")
    cursor.execute("COPY INTO dev.raw_data.session_timestamp FROM @dev.raw_data.blob_stage/session_timestamp.csv;")
    cursor.close()

with DAG(
    dag_id="session_to_snowflake",
    start_date = datetime.datetime(2024,10,18),
    catchup=False,
    tags=['ETL'],
    schedule_interval = '@daily'
) as dag:
    set_stage = set_stage()
    load = load()

    set_stage >> load