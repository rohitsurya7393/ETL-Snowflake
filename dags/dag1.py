from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    "start_date": pendulum.datetime(2025, 9, 14, tz="UTC"),  # set start date explicitly
}

with DAG(
    dag_id = "print_cat",
    default_args=default_args,
    schedule = "@daily",
    catchup = False
) as dag:

    t1 = BashOperator(
        task_id = "print_hello",
        bash_command = "echo 'rohit surya learning airflow'"
    )

    t2 = BashOperator(
        task_id = "run_py",
        bash_command = "python /opt/airflow/dags/ro.py"
    )

    t1 >> t2