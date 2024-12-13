import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from snowflake.connector import connect
from tempfile import NamedTemporaryFile
import logging

# default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    "postgres_to_snowflake_etl",
    default_args=default_args,
    description="ETL process to transfer data from PostgreSQL to Snowflake",
    schedule_interval="@hourly",  # run the DAG every hour
)


# function to extract data from PostgreSQL
def extract_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    sql = "SELECT * FROM public.articles;"
    df = postgres_hook.get_pandas_df(sql)

    logging.info(f"Extracted {len(df)} rows from PostgreSQL")

    kwargs["ti"].xcom_push(key="dataframe", value=df.to_json())


# function to transform the data types from PostgreSQL to Snowflake
def transform_data(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="extract_data", key="dataframe")
    df = pd.read_json(df_json)

    # transform the data types
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")
        elif df[col].dtype == "int64":
            df[col] = df[col].astype("float")
        elif df[col].dtype == "datetime64[ns]":
            df[col] = df[col].astype("string")

    logging.info(f"Transformed data with {len(df)} rows")

    kwargs["ti"].xcom_push(key="transformed_dataframe", value=df.to_json())


# function to load the transformed data to a CSV file
def load_to_csv(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="transform_data", key="transformed_dataframe")
    df = pd.read_json(df_json)

    # write the data to a temporary CSV file
    with NamedTemporaryFile(delete=False, suffix=".csv") as temp:
        df.to_csv(temp.name, index=False)
        logging.info(f"Written data to {temp.name}")

    kwargs["ti"].xcom_push(key="csv_filename", value=temp.name)


# function to upload the CSV file to Snowflake
def upload_to_snowflake(**kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(task_ids="load_to_csv", key="csv_filename")

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = snowflake_hook.get_conn()

    cursor = conn.cursor()
    try:
        # create a stage and copy the data from the CSV file
        cursor.execute(
            """
            CREATE OR REPLACE stage article_stage 
            FILE_FORMAT=(TYPE= 'CSV');
            """
        )
        # upload the CSV file to the stage
        cursor.execute(f"PUT file://{filename} @article_stage;")
        # copy the data from the stage to the target table
        cursor.execute(
            """
            COPY INTO public.articles
            FROM @article_stage
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE';
            """
        )
    finally:
        cursor.close()
        conn.close()


# validate the data in PostgreSQL and Snowflake
def validate_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    # get the count of rows in PostgreSQL
    postgres_count = postgres_hook.get_first("SELECT COUNT(*) FROM public.articles;")[0]

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = snowflake_hook.get_conn()

    cursor = conn.cursor()
    try:
        # get the count of rows in Snowflake
        snowflake_count = cursor.execute("SELECT COUNT(*) FROM articles;").fetchone()[0]
        logging.info(
            f"PostgreSQL count: {postgres_count}, Snowflake count: {snowflake_count}"
        )
        if postgres_count != snowflake_count:
            raise ValueError("Data validation failed")
    finally:
        cursor.close()
        conn.close()


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_csv",
    python_callable=load_to_csv,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task >> upload_task >> validate_task
