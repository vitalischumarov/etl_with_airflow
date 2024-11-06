from airflow import DAG
import pandas as pd
from io import StringIO
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner' : 'Vitali'
}

def loadDataFromSource():
    df1 = pd.read_csv('./dags/source/data1.csv', index_col = 0)
    df2 = pd.read_csv('./dags/source/data2.csv', index_col = 0)
    df3 = pd.read_csv('./dags/source/data3.csv', index_col = 0)
    combinedDataFrame = pd.concat([df1,df2,df3], ignore_index = False)
    
    hook = PostgresHook(postgres_conn_id = 'staging_area_connection')
    
    csv_buffer = StringIO()
    combinedDataFrame.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM person")
            cursor.copy_expert("COPY person FROM STDIN WITH CSV", csv_buffer)
        conn.commit()

def loadDataFromPostgresToDataFrame(ti):
    hook = PostgresHook(postgres_conn_id = 'staging_area_connection')
    records = hook.get_records('SELECT DISTINCT * FROM person')
    df = pd.DataFrame(records, columns=['name','alter','stadt','beruf','gehalt','datum'])
    ti.xcom_push(key = 'DataFrame', value = df)
    
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM person")
        conn.commit()
    print('successful loaded')

def transformation(ti):
    df = ti.xcom_pull(key = 'DataFrame', task_ids = 'loadDataFromPostgresToDataFrame')
    df = df.rename(columns={"gehalt" : 'Gehalt / Jahr'})
    df['Gehalt / Jahr'] = df['Gehalt / Jahr'].round(2)
    df['datum'] = pd.to_datetime(df['datum'], format='mixed')
    df['datum'] = df['datum'].dt.strftime('%Y-%m-%d')
    ti.xcom_push(key = 'DataFrameTransformed', value = df)

def loadDataIntoDataWarehouse(ti):
    hook = PostgresHook(postgres_conn_id = 'dwh_connection')
    df = ti.xcom_pull(key = 'DataFrameTransformed', task_ids = 'transformation')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.copy_expert("COPY person FROM STDIN WITH CSV", csv_buffer)
        conn.commit()

with DAG (
    dag_id = 'PIPELINE',
    start_date = datetime(2024, 11, 3),
    schedule = '@daily',
    default_args = default_args,
    catchup = False
) as dag:
    
    loadDataFromSource = PythonOperator (
        task_id = 'loadDataFromSource',
        python_callable = loadDataFromSource
    )

    loadDataFromPostgresToDataFrame = PythonOperator(
        task_id = 'loadDataFromPostgresToDataFrame',
        python_callable = loadDataFromPostgresToDataFrame
    )

    transformation = PythonOperator(
        task_id = 'transformation',
        python_callable = transformation
    )

    loadDataIntoDataWarehouse = PythonOperator(
        task_id = 'loadDataIntoDataWarehouse',
        python_callable = loadDataIntoDataWarehouse
    )

    loadDataFromSource >> loadDataFromPostgresToDataFrame >> transformation >> loadDataIntoDataWarehouse