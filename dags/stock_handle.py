from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

from pyspark.sql import SparkSession
from tasks.Stock import Stock
from tasks.GlobalUtil import GlobalUtil

WORKFLOW_DAG_ID = 'stock_handle'
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
    description="ETL for stock data",
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
stock = Stock(spark)

t1 = PythonOperator(
        task_id='extract_sp500_tickers',
        python_callable=stock.extract_sp500_tickers,
        dag=dag,
)

start_date = datetime(2020, 1, 1)
end_date = datetime.now()
t2 = PythonOperator(
        task_id='extract_batch_stock',
        python_callable=stock.extract_batch_stock,
        # op_kwargs={'reload': True,
        #        'ticker_file': Stock.DEFAULT_TICKER_FILE,
        #        'start_date': start_date,
        #        'end_date': end_date},
        dag=dag,
)

t3 = PythonOperator(
        task_id='transform_raw_to_fact_stock',
        python_callable=stock.transform_raw_to_fact_stock,
        dag=dag,
)
t4 = PythonOperator(
        task_id='aggregate_fact_to_monthly_fact_stock',
        python_callable=stock.aggregate_fact_to_monthly_fact_stock,
        dag=dag,
)

t1 >> t2 >> t3 >> t4
