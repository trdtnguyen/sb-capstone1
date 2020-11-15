from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

from tasks import covid19_extract
from tasks import covid19_transform

WORKFLOW_DAG_ID = 'covid19_handle'
WORKFLOW_START_DATE = datetime.now() - timedelta(days=1)

#using CRON format
WORKFLOW_SCHEDULE_INTERVAL = None

WORKFLOW_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": WORKFLOW_START_DATE,
    "email": ["sb@opened_capstone.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    WORKFLOW_DAG_ID,
    description="ETL for covid19 data",
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
)

t1 = PythonOperator(
        task_id='extract_us',
        python_callable=covid19_extract.extract_us,
        op_kwargs={'param1': 'value'},
        dag=dag,
)
