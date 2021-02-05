from tasks.GlobalUtil import GlobalUtil

from pyspark.sql import SparkSession, Row

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when

from db.DB import DB
import pandas as pd
from pandas_datareader import data
import configparser
from sqlalchemy.sql import text

import requests
import json
from os import environ as env
from datetime import timedelta, datetime
import pymysql

DEFAULT_TICKER_FILE = 'bol_series.txt'

"""
convert from BOL period to datetime format
year: int
period: month period is a value of M01, M02, ...,M12
"""
def BOL_period_to_date(year, period):
    str_m = period[1:3]
    date = datetime(year, int(str_m), 1)
    return date
"""
Convert from datetime to dateid
"""
def from_date_to_dateid(date: datetime):
    date_str = date.strftime('%Y-%m-%d')
    date_str = date_str.replace('-', '')
    dateid = int(date_str)
    return dateid

class BOL:
    def __init__(self, spark):
        self.GU = GlobalUtil.instance()

        user = self.GU.CONFIG['DATABASE']['MYSQL_USER']
        db_name = self.GU.CONFIG['DATABASE']['MYSQL_DATABASE']
        pw = self.GU.CONFIG['DATABASE']['MYSQL_PASSWORD']
        host = self.GU.CONFIG['DATABASE']['MYSQL_HOST']

        config = configparser.ConfigParser()
        # config.read('config.cnf')
        config.read('../config.cnf')
        # str_conn  = 'mysql+pymysql://root:12345678@localhost/bank'
        str_conn = f'mysql+pymysql://{user}:{pw}@{host}/{db_name}'
        self.db = DB(str_conn)
        self.conn = self.db.get_conn()
        self.logger = self.db.get_logger()
        self.spark = spark
    """
    Read list of interested feature from dim table then extract each feature data
    """
    def extract_BOL(self):
        conn = self.conn
        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['BOL_SERIES_FACT_TABLE_NAME']
        DIM_TABLE_NAME = 'bol_series_dim'

        is_resume_extract = False
        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE
        start_date = datetime(2011, 1, 1)

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        end_date = datetime.now()
        fact_df = self.GU.read_from_db(self.spark, FACT_TABLE_NAME)

        if is_resume_extract:
            # we only compare two dates by month, year excluding time
            if latest_date.month == end_date.month and \
                    latest_date.year == end_date.year:
                print(f'The system has updated data up to year {end_date.year}, '
                      f'month {end_date.month}. No further extract needed.')
                return
            else:
                start_date = latest_date
        start_year = start_date.year
        # Note that the API allow maximum 10 years range

        end_year = end_date.year

        if start_year + 10 < end_year:
            start_year = end_year - 10
        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Read series from database
        #########
        print('Read series ...', end=  " ")
        dim_df = self.GU.read_from_db(self.spark, DIM_TABLE_NAME)


        #########
        ### Step 4 Extract data from BOL using API
        #########
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        insert_list = []
        series_ids = self.GU.rows_to_array(dim_df, 'series_id')

        bol_df = data.DataReader(series_ids, 'fred', start_date_str, end_date_str)
        for i, row in bol_df.iterrows():
            date = i
            dateid = from_date_to_dateid(i)
            for j in range(len(series_ids)):
                insert_val = {}
                insert_val['date'] = i
                insert_val['dateid'] = from_date_to_dateid(i)
                insert_val['series_id'] = series_ids[j]
                insert_val['value'] = row[j]

                insert_list.append(insert_val)

        ####################################
        # Step 5 Write to Database
        ####################################

        df = pd.DataFrame(insert_list)
        print("Extract BOL data Done.")
        print(f"Insert to table {FACT_TABLE_NAME} ...", end=' ')
        try:
            df.to_sql(FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
            # manually insert a dictionary will not work due to the limitation number of records insert
            # results = conn.execute(table.insert(), insert_list)
        except ValueError:
            logger.error(f'Error Query when extracting data for {FACT_TABLE_NAME} table')

        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

    def transform_raw_to_fact_bol(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'bol_raw'
        FACT_TABLE_NAME = 'bol_series_fact'

        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)
        # 1. Transform from raw to fact table
        end_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        #Note that the raw dataset from datasource is monthly update
        if is_resume_extract:
            if latest_date.year == end_date.year and latest_date.month == end_date.month:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return

        raw_df = self.GU.read_from_db(self.spark, RAW_TABLE_NAME)
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)
        #########
        ### Step 3 Transform. Add dateid and date column into the raw table
        #########

        # Add date column
        print(raw_df.count())
        raw_df = raw_df.select(col('series_id'), col('year'), col('period'), col('value'), col('footnotes')).distinct()
        print(raw_df.count())

        date_udf = udf(lambda year, period: BOL_period_to_date(year, period), DateType())
        df = raw_df.withColumn('date', date_udf(raw_df['year'], raw_df['period']))
        # Add dateid column
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        df = df.withColumn('dateid', dateid_udf(df['date']))
        print(df.count())
        df = df.drop_duplicates("series_id", "dateid")
        print(df.count())
        # Reorder columns to match the schema
        df = df.select(df['dateid'], df['series_id'], df['date'], df['value'], df['footnotes'])
        print(df.count())
        df = df.distinct()
        print(df.count())
        df.createOrReplaceTempView("tem")
        s = "SELECT DISTINCT * FROM tem"
        df_tem = self.spark.sql(s)
        print(df_tem.count())
        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

#extract_BOL(conn, logger, 2010, 2020)
#self.GU = GlobalUtil.instance()
# spark = SparkSession \
#     .builder \
#     .appName("sb-miniproject6") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
# # Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
#
# bol = BOL(spark)
#
# bol.extract_BOL()
# bol.transform_raw_to_fact_bol()
