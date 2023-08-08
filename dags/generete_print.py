from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def generate_output():
    output = "Hello, Airflow!"
    return output

def print_output(output):
    print(output)

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('generete_print', default_args=default_args, schedule_interval='@daily') as dag:
    generate_operator = PythonOperator(
        task_id='generate_output',
        python_callable=generate_output
    )

    print_operator = PythonOperator(
        task_id='print_output',
        python_callable=print_output,
        op_kwargs={'output': "{{ ti.xcom_pull(task_ids='generate_output') }}"}
    )

    generate_operator >> print_operator
