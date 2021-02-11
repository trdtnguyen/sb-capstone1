from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil
from tasks.Covid import Covid
from tasks.Stock import Stock
from tasks.BOL import BOL
from tasks.Consolidate import Consolidation

WORKFLOW_DAG_ID = 'covid19cor_etl'
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
    description="ETL for covid19cor",
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

### COVID Data sources
covid = Covid(spark)

covid_us_t1 = PythonOperator(
        task_id='extract_us',
        python_callable=covid.extract_us,
        dag=dag,
)
covid_us_t2 = PythonOperator(
        task_id='transform_raw_to_fact_us',
        python_callable=covid.transform_raw_to_fact_us,
        dag=dag,
)
covid_us_t3 = PythonOperator(
        task_id='aggregate_fact_to_monthly_fact_us',
        python_callable=covid.aggregate_fact_to_monthly_fact_us,
        dag=dag,
)

covid_us_t1 >> covid_us_t2 >> covid_us_t3

covid_global_t1 = PythonOperator(
        task_id='transform_raw_to_dim_country',
        python_callable=covid.transform_raw_to_dim_country,
        dag=dag,
)
covid_global_t2 = PythonOperator(
        task_id='extract_global',
        python_callable=covid.extract_global,
        dag=dag,
)
covid_global_t3 = PythonOperator(
        task_id='transform_raw_to_fact_global',
        python_callable=covid.transform_raw_to_fact_global,
        dag=dag,
)
covid_global_t4 = PythonOperator(
        task_id='aggregate_fact_to_monthly_fact_global',
        python_callable=covid.aggregate_fact_to_monthly_fact_global,
        dag=dag,
)

covid_global_t1 >> covid_global_t2 >> covid_global_t3 >> covid_global_t4

covid_aggregate_t1 = PythonOperator(
        task_id='aggregate_fact_to_sum_fact',
        python_callable=covid.aggregate_fact_to_sum_fact,
        dag=dag,
)
covid_aggregate_t2 = PythonOperator(
        task_id='aggregate_fact_to_sum_monthly_fact',
        python_callable=covid.aggregate_fact_to_sum_monthly_fact,
        dag=dag,
)

[covid_us_t3, covid_global_t4] >> covid_aggregate_t1 >> covid_aggregate_t2

# Stock Data Source
stock = Stock(spark)

stock_t1 = PythonOperator(
        task_id='extract_major_stock_indexes',
        python_callable=stock.extract_major_stock_indexes,
        dag=dag,
)
stock_t2 = PythonOperator(
        task_id='aggregate_fact_to_monthly_fact_stock_index',
        python_callable=stock.aggregate_fact_to_monthly_fact_stock_index,
        dag=dag,
)

stock_t1 >> stock_t2

### BOL Data Source
bol = BOL(spark)
bol_t1 = PythonOperator(
        task_id='extract_BOL',
        python_callable=bol.extract_BOL,
        dag=dag,
)

# Consolidate
consolidate = Consolidation(spark)
consolidate_t1 = PythonOperator(
        task_id='consolidate_covid_stock',
        python_callable=consolidate.consolidate_covid_stock,
        dag=dag,
)

consolidate_t2 = PythonOperator(
        task_id='aggregate_covid_stock_monthly_fact',
        python_callable=consolidate.aggregate_covid_stock_monthly_fact,
        dag=dag,
)

consolidate_t3 = PythonOperator(
        task_id='consolidate_covid_stock_bol',
        python_callable=consolidate.consolidate_covid_stock_bol,
        dag=dag,
)

[covid_aggregate_t2, stock_t2, bol_t1] >> consolidate_t1 >> consolidate_t2 >> consolidate_t3