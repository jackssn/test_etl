import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator


SCRIPT_ABSPATH = "<your_abs_path_of_script>"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
}

dag = DAG(
    dag_id='data_loading_task',
    description='This task makes incremental data loading.',
    default_args=default_args,
    schedule_interval="@hourly",
)

task = BashOperator(
    task_id='run_script_to_load_data',
    bash_command=f'python3 {SCRIPT_ABSPATH}/script.py',
    dag=dag,
)

