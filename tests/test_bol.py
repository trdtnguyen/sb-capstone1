"""
Test unit for ETL Stock
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import pytest

from tasks.BOL import BOL
from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil

GU = GlobalUtil.instance()

spark = SparkSession \
    .builder \
    .appName(GU.CONFIG['CORE']['PROJECT_NAME']) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

bol = BOL(spark)

RAW_TABLE_NAME = 'bol_raw'
DIM_TABLE_NAME = 'bol_series_dim'
FACT_TABLE_NAME = 'bol_series_fact'


"""
Note that dim table is created during executing sql/create_table.sql
"""
def test_dim_table_exist():
    dim_df = GU.read_from_db(spark, DIM_TABLE_NAME)
    assert dim_df.count() > 0


def test_extract_BOL():
    bol.extract_BOL()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, RAW_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    df = GU.read_from_db(spark, RAW_TABLE_NAME)
    assert df.count() > 0


def test_transform_raw_to_fact_bol():
    bol.transform_raw_to_fact_bol()

    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, FACT_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    df = GU.read_from_db(spark, FACT_TABLE_NAME)
    assert df.count() > 0