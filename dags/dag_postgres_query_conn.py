from airflow import DAG
from datetime import datetime
from postgres_query_operator_conn import PostgresQueryConnOperator

dag = DAG('postgres_query_conn', start_date=datetime.now(), schedule_interval='@daily')

query_postgres = PostgresQueryConnOperator(
    task_id='query_postgres',
    sql='SELECT * FROM table_test',
    conn_id='postgres_default',
    path='/opt/airflow/data',
    file_name='test_data_airflow',
    dag=dag
)

query_postgres