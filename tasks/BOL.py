from tasks.GlobalUtil import GlobalUtil

from pyspark.sql import SparkSession, Row

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when

from db.DB import DB
import pandas as pd
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
        RAW_TABLE_NAME = 'bol_raw'
        DIM_TABLE_NAME = 'bol_series_dim'

        is_resume_extract = False
        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE
        start_date = datetime(2011, 1, 1)

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, RAW_TABLE_NAME)

        end_date = datetime.now()
        raw_df = self.GU.read_from_db(self.spark, RAW_TABLE_NAME)

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
        latest_df = self.GU.update_latest_data(latest_df, RAW_TABLE_NAME, end_date)



        # # 1. Resuming extract data.
        # s = text("SELECT year "
        #          f"FROM {RAW_TABLE_NAME} "
        #          "ORDER BY year DESC "
        #          "LIMIT 1")
        #
        # # try:
        #     ret_list = conn.execute(s).fetchall()
        #     df_tem = pd.DataFrame(ret_list)
        #     if len(df_tem) > 0:
        #         latest_year = df_tem.iloc[0][0]
        #         if latest_year > end_year:
        #             print(f'database last update on {latest_year} '
        #                   f'that is later than the end date {end_year}. No further extract needed')
        #             return
        #         else:
        #             print(f'set start_year to {latest_year}')
        #             start_year = latest_year
        #
        # except pymysql.OperationalError as e:
        #     logger.error(f'Error Query when get latest year in {RAW_TABLE_NAME}')
        #     print(e)

        #########
        ### Step 3 Read series from database
        #########
        print('Read series ...', end=  " ")
        dim_df = self.GU.read_from_db(self.spark, DIM_TABLE_NAME)
        # s = text("SELECT series_id "
        #          f"FROM {DIM_TABLE_NAME} "
        #          )
        #     #series_ids = ['CUUR0000SA0', 'SUUR0000SA0']
        # try:
        #     ret_list = conn.execute(s).fetchall()
        #     df_tem = pd.DataFrame(ret_list)
        #     if len(df_tem) > 0:
        #         series_ids_tem = df_tem.values.tolist() # Get array of array
        #         series_ids = [arr[0] for arr in series_ids_tem]
        #     else:
        #         logger.error('The series_ids is empty')
        #         print('The series_ids is empty')
        #         return
        #
        # except pymysql.OperationalError as e:
        #     logger.error('Error Query when get latest year in BOL_raw')
        #     print(e)
        # print("Done. Number of series: ", len(series_ids))

        #########
        ### Step 4 Extract data from BOL using API
        #########
        insert_list = []
        series_ids = self.GU.rows_to_array(dim_df, 'series_id')

        # API_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        API_url = self.GU.CONFIG['BOL']['BOL_API_URL']
        data = json.dumps({"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)})

        headers = {'Content-type': 'application/json'}
        p = requests.post(API_url, data=data, headers=headers)
        json_data = json.loads(p.text)
        for series in json_data['Results']['series']:
            seriesID = series['seriesID']
            for item in series['data']:
                insert_val = {}
                insert_val['series_id'] = seriesID
                insert_val['year'] = item['year']
                insert_val['period'] = item['period']
                insert_val['value'] = float(item['value'])

                footnotes = ""
                for footnote in item['footnotes']:
                    if footnote:
                        footnotes = footnotes + footnote['text'] + ','
                insert_val['footnotes'] = footnotes
                insert_list.append(insert_val)

        ####################################
        # Step 5 Write to Database
        ####################################
        df = pd.DataFrame(insert_list)
        print("Extract data Done.")
        print("Insert to database...", end=' ')
        try:
            df.to_sql(RAW_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
            # manually insert a dictionary will not work due to the limitation number of records insert
            # results = conn.execute(table.insert(), insert_list)
        except ValueError:
            logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')

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
        date_udf = udf(lambda year, period: BOL_period_to_date(year, period), DateType())
        df = raw_df.withColumn('date', date_udf(raw_df['year'], raw_df['period']))
        # Add dateid column
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        df = df.withColumn('dateid', dateid_udf(df['date']))

        # Reorder columns to match the schema
        df = df.select(df['dateid'], df['series_id'], df['date'], df['value'], df['footnotes'])

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
