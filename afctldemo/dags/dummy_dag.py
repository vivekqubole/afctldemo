# STEP - 1

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

# STEP - 2

default_args = {
        'owner': 'qubole',
        'depends_on_past': False,
        'start_date': datetime(2020, 4, 13),
        'retries': 0,
}


# STEP - 3

dag = DAG(dag_id="DAG-1", default_args=default_args, catchup=False, schedule_interval='@once')


# STEP - 4

dummy_start = DummyOperator(task_id='dummy_start', dag=dag)
dummy_end = DummyOperator(task_id='dummy_end', dag=dag)


# STEP - 5
dummy_start >> dummy_end




