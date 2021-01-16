from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from tasks.Covid import Covid

WORKFLOW_DAG_ID = 'covid19_us_handle'
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
    description="ETL for covid19 us data",
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
)

spark = SparkSession \
    .builder \
    .appName("sb-miniproject6") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
covid = Covid(spark)

t1 = PythonOperator(
        task_id='extract_us',
        python_callable=covid.extract_us,
        dag=dag,
)
t2 = PythonOperator(
        task_id='transform_raw_to_fact_us',
        python_callable=covid.transform_raw_to_fact_us,
        dag=dag,
)
t3 = PythonOperator(
        task_id='aggregate_fact_to_monthly_fact_us',
        python_callable=covid.aggregate_fact_to_monthly_fact_us,
        dag=dag,
)

t1 >> t2 >> t3
