from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from pyspark.sql import SparkSession
from tasks.BOL import BOL
from tasks.GlobalUtil import GlobalUtil

WORKFLOW_DAG_ID = 'bol_handle'
WORKFLOW_START_DATE = datetime.now() - timedelta(days=1)

# using CRON format
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
    description="ETL for BOL data",
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
)
GU = GlobalUtil.instance()
spark = SparkSession \
    .builder \
    .appName(GU.CONFIG['CORE']['PROJECT_NAME']) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
bol = BOL(spark)
# start_year = 2010
# end_year = datetime.now().year

t1 = PythonOperator(
        task_id='extract_BOL',
        python_callable=bol.extract_BOL,
        # op_kwargs={'start_year': start_year,
        #            'end_year': end_year
        #           },
        dag=dag,
)

t2 = PythonOperator(
        task_id='transform_raw_to_fact_bol',
        python_callable=bol.transform_raw_to_fact_bol,
        dag=dag,
)

t1 >> t2
