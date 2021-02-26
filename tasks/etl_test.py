"""
Self ETL for developer
"""
__version__ = '0.2'
__author__ = 'Dat Nguyen'

import sys
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


opts1 = [opt for opt in sys.argv[1:] if opt.startswith('-')]
opts2 = [opt for opt in sys.argv[1:] if opt.startswith('--')]
args = [arg for arg in sys.argv[1:] if not arg.startswith('-')]

# query_date = datetime.now()
query_date = datetime(2021, 2, 16)
if '-h' in  opts1:
    print(f'Test ETL tasks. Usage: {sys.argv[0]} (-h | --help) <argument> ')
    print('Argument: date the end date in format yyyy-mm-dd')
    exit(0)
elif len(args) == 1:
    try:
       query_date = datetime.strptime(args[0], '%Y-%m-%d')
    except Exception as e:
        print(e)
        exit(1)

spark = SparkSession \
    .builder \
    .appName("Covidcor-self-test") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

etl_covid = True
etl_stock = True
etl_bol = True
etl_consolid = True

#query_date = datetime(2021, 2, 16)
# query_date = datetime(2021, 2, 24)
#### Covid
if etl_covid:
    covid = Covid(spark)
    covid.extract_us(query_date)
    covid.transform_raw_to_fact_us()
    covid.aggregate_fact_to_monthly_fact_us()

    ###################
    covid.extract_global(query_date)
    covid.transform_raw_to_fact_global()
    covid.aggregate_fact_to_monthly_fact_global()
    # # # #
    covid.aggregate_fact_to_sum_fact()
    covid.aggregate_fact_to_sum_monthly_fact()

#### Stock
if etl_stock:
    stock = Stock(spark)
    stock.extract_major_stock_indexes(query_date)
    stock.aggregate_fact_to_monthly_fact_stock_index()

    # stock.extract_sp500_tickers()
    # stock.extract_batch_stock()
    # stock.transform_raw_to_fact_stock()
    # stock.aggregate_fact_to_monthly_fact_stock()

#### BOL
if etl_bol:
    bol = BOL(spark)
    bol.extract_BOL(query_date)

### Consolidate
if etl_consolid:
    consolidate = Consolidation(spark)
    consolidate.consolidate_covid_stock()
    consolidate.aggregate_covid_stock_monthly_fact()
    consolidate.consolidate_covid_stock_bol()