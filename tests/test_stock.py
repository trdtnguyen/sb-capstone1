"""
Test unit for ETL Stock
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from tasks.Stock import Stock
from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil

#sys.path.append("/home/dtn/sb-capstone1/tasks")
#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# print (sys.path)

GU = GlobalUtil.instance()

spark = SparkSession \
    .builder \
    .appName(GU.CONFIG['CORE']['PROJECT_NAME']) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

stock = Stock(spark)

RAW_TABLE_NAME = 'stock_price_raw'
TICKER_TABLE_NAME = 'stock_ticker_raw'
FACT_TABLE_NAME = 'stock_price_fact'
MONTHLY_FACT_TABLE_NAME = 'stock_price_monthly_fact'

def test_extract_sp500_tickers():
    stock.extract_sp500_tickers()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, TICKER_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    raw_df = GU.read_from_db(spark, TICKER_TABLE_NAME)
    assert raw_df.count() > 0

def test_extract_batch_stock():
    stock.extract_batch_stock()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, RAW_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    df = GU.read_from_db(spark, RAW_TABLE_NAME)
    assert df.count() > 0


def test_transform_raw_to_fact_stock():
    stock.transform_raw_to_fact_stock()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, FACT_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    df = GU.read_from_db(spark, FACT_TABLE_NAME)
    assert df.count() > 0

def test_aggregate_fact_to_monthly_fact_stock():
    stock.aggregate_fact_to_monthly_fact_stock()

    stock.transform_raw_to_fact_stock()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, MONTHLY_FACT_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    df = GU.read_from_db(spark, MONTHLY_FACT_TABLE_NAME)
    assert df.count() > 0
