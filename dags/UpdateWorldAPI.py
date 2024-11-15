from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from pandas import Timestamp
import pandas as pd
import logging
import requests


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_worlds_info():
    url = f"https://restcountries.com/v3/all"
    response = requests.get(url)
    data = response.json()
    records = []

    for row in data:
        records.append([row["name"]["official"], row["population"], row["area"]])
    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country varchar(255),
    population bigint,
    area float
);""")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);"
            print(sql)
            cur.execute(sql, (r[0], r[1], r[2]))
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'UpdateWorldAPI',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule_interval='30 6 * * 6',
) as dag:

    results = get_worlds_info()
    load("kyongjin1234", "world_info", results)
