"""
Self ETL for developer
"""
__version__ = '0.2'
__author__ = 'Dat Nguyen'


from tasks.Covid import Covid
from tasks.Stock import Stock
from tasks.BOL import BOL
from tasks.Consolidate import Consolidation
from tasks.GlobalUtil import GlobalUtil
from pyspark.sql import SparkSession

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when

from db.DB import DB
import pandas as pd
from pandas_datareader import wb
import configparser
from pandas_datareader import data
from sqlalchemy.sql import text
from datetime import timedelta, datetime

import requests
import bs4 as bs
import math # for isnan()


spark = SparkSession \
    .builder \
    .appName("Covidcor-self-test") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

etl_covid = True
etl_stock = False
etl_bol = False
etl_consolid = False
query_date = datetime(2021, 2, 9)
#### Covid
if etl_covid:
    covid = Covid(spark)
    covid.extract_us(query_date)
    # covid.transform_raw_to_fact_us()
    # covid.aggregate_fact_to_monthly_fact_us()
    #
    # covid.transform_raw_to_dim_country()
    # covid.extract_global()
    # covid.transform_raw_to_fact_global()
    # covid.aggregate_fact_to_monthly_fact_global()
    # # #
    # covid.aggregate_fact_to_sum_fact()
    # covid.aggregate_fact_to_sum_monthly_fact()

#### Stock
if etl_stock:
    stock = Stock(spark)
    stock.extract_major_stock_indexes()
    stock.aggregate_fact_to_monthly_fact_stock_index()

    # stock.extract_sp500_tickers()
    # stock.extract_batch_stock()
    # stock.transform_raw_to_fact_stock()
    # stock.aggregate_fact_to_monthly_fact_stock()

#### BOL
if etl_bol:
    bol = BOL(spark)
    bol.extract_BOL()

### Consolidate
if etl_consolid:
    consolidate = Consolidation(spark)
    consolidate.consolidate_covid_stock()
    consolidate.aggregate_covid_stock_monthly_fact()
    consolidate.consolidate_covid_stock_bol()