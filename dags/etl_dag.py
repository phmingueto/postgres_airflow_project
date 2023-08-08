from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator


def extract_csv():
    path = '/opt/airflow/data/dataset_teste_Just_BI.csv'
    df = pd.read_csv(path)
    return df


def head_df(df):
    df_head = df.head(5)
    return df_head


def load_df(df):
    df.to_csv('/opt/airflow/data/dataset_teste.csv')


# Definindo a DAG e os argumentos
with DAG(
        "etl_dag",
        start_date=datetime.now(),
) as dag:
    # Organizando Tasks
    extract_csv_operator = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
        dag=dag

    )
    head_df_operator = PythonOperator(
        task_id='head_df',
        python_callable=head_df,
        dag=dag,
        op_args=[extract_csv_operator.output]
    )

    load_df_operator = PythonOperator(
        task_id='load_df',
        python_callable=load_df,
        dag=dag,
        op_args=[head_df_operator.output]
    )

extract_csv_operator >> head_df_operator >> load_df_operator
