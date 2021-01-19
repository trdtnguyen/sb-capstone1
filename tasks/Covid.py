"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import os
from os import environ as env
import sys
#sys.path.append(os.path.join(os.path.dirname(__file__), ".", ".."))

from tasks.GlobalUtil import GlobalUtil
# import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkFiles  # for reading csv file from https
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when

from db.DB import DB
import configparser
from datetime import timedelta, datetime

print (sys.path)


"""
   Convert from string 'yyyy/mm/dd' to datetime
"""
def convert_date(in_date: str):
    d_list = in_date.split('/')
    year = int('20' + d_list[2])
    month = int(d_list[0])
    day = int(d_list[1])
    # out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]
    out_date = datetime(year, month, day)
    return out_date


"""
Convert from datetime to dateid
"""
def from_date_to_dateid(date: datetime):
    date_str = date.strftime('%Y-%m-%d')
    date_str = date_str.replace('-', '')
    dateid = int(date_str)
    return dateid

def create_first_day_of_month(month: int, year: int):
    return datetime(year, month, 1)

def get_month_name(date):
    month_name = date.strftime('%B')
    return month_name

class Covid:
    """Constructor

    """

    def __init__(self, spark: SparkSession):
        user = env.get('MYSQL_USER')
        db_name = env.get('MYSQL_DATABASE')
        pw = env.get('MYSQL_PASSWORD')
        host = env.get('MYSQL_HOST')

        # str_conn  = 'mysql+pymysql://root:12345678@localhost/bank'
        str_conn = f'mysql+pymysql://{user}:{pw}@{host}/{db_name}'

        print('Connect string in constructor: ', str_conn)
        self.db = DB(str_conn)
        self.conn = self.db.get_conn()
        self.logger = self.db.get_logger()
        # spark
        self.spark = spark

        self.GU = GlobalUtil.instance()

    def extract_us(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_us_raw'
        DIM_TABLE_NAME = 'covid19_us_dim'
        is_resume_extract = False
        date_col = 12  # the index of date column

        #######################################
        # Step 1 Read CSV file from datasource to Spark DataFrame
        #######################################
        url1 = self.GU.CONFIG['COVID19']['COVID19_CONFIRMED_US_URL']
        # confirmed_us_df = pd.read_csv(url1)
        file_name1 = os.path.basename(url1)

        # Unlike pandas, read csv file from https in Spark require more steps
        self.spark.sparkContext.addFile(url1)
        # confirmed_us_df = self.spark.read.csv(url1, header=True, inferSchema=True) # This will not work
        confirmed_us_df = self.spark.read.csv('file://' + SparkFiles.get(file_name1), header=True, inferSchema=True)
        ncols1 = len(confirmed_us_df.columns)
        # confirmed_us_df.describe().show()

        url2 = self.GU.CONFIG['COVID19']['COVID19_DEATH_US_URL']
        file_name2 = os.path.basename(url2)
        # death_us_df = pd.read_csv(url2)
        self.spark.sparkContext.addFile(url2)
        # death_us_df = self.spark.read.csv(url2, header=True, inferSchema=True)
        death_us_df = self.spark.read.csv('file://' + SparkFiles.get(file_name2), header=True, inferSchema=True)
        ncols2 = len(death_us_df.columns)
        # death_us_df.describe().show()

        # Get latest date from raw table
        begin_date_str = death_us_df.schema.names[date_col]
        begin_date = convert_date(begin_date_str)
        end_date_str = death_us_df.schema.names[-1]
        end_date = convert_date(end_date_str)

        #######################################
        # Step 2 Read from database to determine the last written data point
        #######################################

        # driver_name = 'com.mysql.jdbc.Driver' # Old driver

        print('Read from database ...')
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, RAW_TABLE_NAME)
        print(f'is_resume_extract={is_resume_extract}')
        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming extract
                start_index = date_col + (end_date.day - latest_date.day)
                confirmed_us_df = confirmed_us_df.select(
                    confirmed_us_df['UID'], confirmed_us_df['iso2'], confirmed_us_df['iso3'],
                    confirmed_us_df['code3'], confirmed_us_df['FIPS'], confirmed_us_df['Admin2'],
                    confirmed_us_df['Province_State'], confirmed_us_df['Country_Region'],
                    confirmed_us_df['Lat'], confirmed_us_df['Long_'],
                    confirmed_us_df['Combined_Key'],
                    *(confirmed_us_df.columns[(start_index - 1):])
                )
                death_us_df = death_us_df.select(
                    death_us_df['UID'], death_us_df['iso2'], death_us_df['iso3'],
                    death_us_df['code3'], death_us_df['FIPS'], death_us_df['Admin2'],
                    death_us_df['Province_State'], death_us_df['Country_Region'],
                    death_us_df['Lat'], death_us_df['Long_'],
                    death_us_df['Combined_Key'], death_us_df['Population'],
                    *(death_us_df.columns[start_index:])
                )

        #########
        ### Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, RAW_TABLE_NAME, end_date)
        print('Done.')

        #######################################
        # Step 3 Transform data
        #######################################

        print('Extract remain info from death_us_df ...', end=' ')
        death_us_df = death_us_df.cache()
        if not is_resume_extract:
            dim_df = death_us_df.select(
                death_us_df['UID'], death_us_df['iso2'], death_us_df['iso3'],
                death_us_df['code3'], death_us_df['FIPS'], death_us_df['Admin2'],
                death_us_df['Province_State'], death_us_df['Country_Region'],
                death_us_df['Lat'], death_us_df['Long_'],
                death_us_df['Combined_Key'], death_us_df['Population']
            )
            latest_df = self.GU.update_latest_data(latest_df, DIM_TABLE_NAME, end_date)
        # dim_df.show()
        ####################################
        ## Transopse and Join
        ####################################
        by_cols1 = ['UID', 'iso2', 'iso3',
                    'code3', 'FIPS', 'Admin2',
                    'Province_State', 'Country_Region', 'Lat', 'Long_',
                    'Combined_Key']
        by_cols2 = by_cols1.copy()
        by_cols2.append('Population')

        trans_df1 = self.GU.transpose_columns_to_rows(confirmed_us_df,
                                                 by_cols1, 'date', 'confirmed')
        trans_df2 = self.GU.transpose_columns_to_rows(death_us_df,
                                                 by_cols2, 'date', 'deaths')
        df = trans_df2.join(trans_df1, (trans_df1.UID == trans_df2.UID) & (trans_df1.date == trans_df2.date)) \
            .select(
            trans_df2['UID'], trans_df2['iso2'], trans_df2['iso3'],
            trans_df2['code3'], trans_df2['FIPS'], trans_df2['Admin2'],
            trans_df2['Province_State'], trans_df2['Country_Region'],
            trans_df2['Lat'], trans_df2['Long_'], trans_df2['Combined_Key'],
            trans_df2['date'], trans_df1['confirmed'], trans_df2['deaths']
        )
        # Refine the date column from 'yyyy/mm/dd' to 'yyyy-mm-dd'
        date_udf = udf(lambda d: convert_date(d), DateType())
        df = df.withColumn('date', date_udf(df['date']))
        #if __debug__:
        #    df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME} ...')
        self.GU.write_latest_data(latest_df, logger)

        if not is_resume_extract:
            print(f'Write to table {DIM_TABLE_NAME} ...')
            self.GU.write_to_db(dim_df, DIM_TABLE_NAME, logger)

        print(f'Write to table {RAW_TABLE_NAME} ...')
        self.GU.write_to_db(df, RAW_TABLE_NAME, logger)

    def extract_global(self):
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'

        is_resume_extract = False
        date_col = 4  # the index of date column in the df

        #######################################
        # Step 1 Read CSV file from datasource to Spark DataFrame
        #######################################
        url1 = self.GU.CONFIG['COVID19']['COVID19_CONFIRMED_GLOBAL_URL']
        file_name1 = os.path.basename(url1)
        self.spark.sparkContext.addFile(url1)
        confirmed_df = self.spark.read.csv('file://' + SparkFiles.get(file_name1), header=True, inferSchema=True)

        url2 = self.GU.CONFIG['COVID19']['COVID19_DEATH_GLOBAL_URL']
        file_name2 = os.path.basename(url2)
        self.spark.sparkContext.addFile(url2)
        death_df = self.spark.read.csv('file://' + SparkFiles.get(file_name2), header=True, inferSchema=True)

        # Get latest date from raw table
        begin_date_str = death_df.schema.names[date_col]
        begin_date = convert_date(begin_date_str)
        end_date_str = death_df.schema.names[-1]
        end_date = convert_date(end_date_str)

        COL_NAMES = ['Province_State', 'Country_Region', 'Lat', 'Long_'
                                                                'date', 'confirmed', 'deaths']
        #######################################
        # Step 2 Read from database to determine the last written data point
        #######################################
        print('Read from database ...')
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, RAW_TABLE_NAME)
        latest_df = latest_df.cache()
        latest_df.count()

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming extract
                start_index = date_col + (latest_date.day - begin_date.day)
                confirmed_df = confirmed_df.select(
                    confirmed_df['Province/State'], confirmed_df['Country/Region'],
                    confirmed_df['Lat'], confirmed_df['Long_'],
                    *(confirmed_df.columns[start_index:])
                )
                death_df = death_df.select(
                    death_df['Province/State'], death_df['Country/Region'],
                    death_df['Lat'], death_df['Long_'],
                    *(death_df.columns[start_index:])
                )

        #########
        ### Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, RAW_TABLE_NAME, end_date)

        print('Done.')
        #######################################
        # Step 3 Transform data
        #######################################
        by_cols = ['Province/State', 'Country/Region', 'Lat', 'Long']
        trans_df1 = self.GU.transpose_columns_to_rows(confirmed_df,
                                                 by_cols, 'date', 'confirmed')
        trans_df2 = self.GU.transpose_columns_to_rows(death_df,
                                                 by_cols, 'date', 'deaths')
        df = trans_df2.join(trans_df1, (trans_df1['Country/Region'] == trans_df2['Country/Region']) & (
                    trans_df1.date == trans_df2.date)) \
            .select(
            trans_df2['Province/State'], trans_df2['Country/Region'],
            trans_df2['Lat'], trans_df2['Long'],
            trans_df2['date'], trans_df1['confirmed'], trans_df2['deaths']
        )
        # Refine the date column from 'yyyy/mm/dd' to 'yyyy-mm-dd'
        date_udf = udf(lambda d: convert_date(d), DateType())
        df = df.withColumn('date', date_udf(df['date']))
        df = df.withColumnRenamed('Province/State', 'Province_State') \
            .withColumnRenamed('Country/Region', 'Country_Region') \
            .withColumnRenamed('Long', 'Long_')

        # if __debug__:
        #     df.show()
        # filter null
        before = df.count()
        df = df.where(df['Lat'].isNotNull())
        after = before = df.count()
        print(f'filtered out {(before - after)} rows with Lat is NULL')

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print(f'Write to table {RAW_TABLE_NAME}...')
        self.GU.write_to_db(df, RAW_TABLE_NAME, logger)

        print('Done.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_fact_us() ->
        aggregate_fact_to_monthly_fact_us() (this)
    """

    def aggregate_fact_to_monthly_fact_us(self):
        conn = self.conn
        logger = self.logger
        FACT_TABLE_NAME = 'covid19_us_fact'
        MONTHLY_FACT_TABLE_NAME = 'covid19_us_monthly_fact'

        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE

        print(f'Aggregate data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, MONTHLY_FACT_TABLE_NAME)


        end_date_arr = latest_df.filter(latest_df['table_name'] == FACT_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        fact_df = self.GU.read_from_db(self.spark, FACT_TABLE_NAME)

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming transform
                before = fact_df.count()
                fact_df = fact_df.filter(
                    fact_df['date'] > latest_date
                )
                after = fact_df.count()
                print(f'Skipped {(before - after)} rows')

        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)
        s = "SELECT UID, YEAR(date), MONTH(date), SUM(confirmed) , SUM(deaths)" + \
            f"FROM {FACT_TABLE_NAME} " + \
            "GROUP BY UID, YEAR(date), MONTH(date)"

        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        df = df.toDF('UID', 'year', 'month', 'confirmed', 'deaths')

        # Add column 'dateid' and 'date'
        first_day_udf = udf(lambda y, m: datetime(y, m, 1), DateType())
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())

        #month_name_udf = udf(lambda d: get_month_name(d), StringType())
        month_name_udf = udf(lambda d: d.strftime('%B'), StringType())
        df = df.withColumn('date', first_day_udf(df['year'], df['month']))
        df = df.withColumn('dateid', dateid_udf(df['date'])) \
            .withColumn('month_name', month_name_udf(df['date']))

        # Reorder the columns to match the schema
        df = df.select(df['dateid'], df['UID'], df['date'], df['year'],
                       df['month'], df['month_name'], df['confirmed'], df['deaths'])

        # df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_fact_us() ->
        this
    """

    def transform_raw_to_fact_us(self):

        logger = self.logger
        RAW_TABLE_NAME = 'covid19_us_raw'
        FACT_TABLE_NAME = 'covid19_us_fact'
        is_resume_extract = False

        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE

        print(f'Transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}.')

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)


        end_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        raw_df = self.GU.read_from_db(self.spark, RAW_TABLE_NAME)

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming transform
                before = raw_df.count()
                raw_df = raw_df.filter(
                    raw_df['date'] > latest_date
                )
                after = raw_df.count()
                print(f'Skipped {(before - after)} rows')

        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        # Filtering specific columns
        df = raw_df.select(
            col('UID'), col('date'), col('confirmed'), col('deaths')).distinct()
        # Add column "dateid" computed from "date"
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        df = df.withColumn('dateid', dateid_udf(raw_df['date']))

        #if __debug__:
        #    df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)

        print('Done.')

    """
    Read local CSV file and write on corresponding table in database
    Dependencies:
        None. This function could work independently with others
    """

    def transform_raw_to_dim_country(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'
        DIM_TABLE_NAME = 'country_dim'
        #COUNTRY_TABLE_NAME = 'world.country'
        COUNTRY_TABLE_NAME = 'world_country'

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, DIM_TABLE_NAME)
        if is_resume_extract:
            print(f'Dimensional table {DIM_TABLE_NAME} has been created. No further extract needed.')
            return

        #######################################
        # Step 2 Read CSV file from external data to Spark DataFrame
        #######################################
        url1 = os.path.join(self.GU.PROJECT_PATH, 'data')
        file1 = os.path.join(url1, self.GU.CONFIG['COVID19']['WORLD_COUNTRY_FILE'])

        s = "SELECT DISTINCT code, Name, Lat, Long_, Continent, " + \
            "Region, SurfaceArea, IndepYear, Population, " + \
            "LifeExpectancy, GNP, LocalName, GovernmentForm, " + \
            "HeadOfState, Capital, Code2 " + \
            f"FROM {COUNTRY_TABLE_NAME}, {RAW_TABLE_NAME} " + \
            f"WHERE {COUNTRY_TABLE_NAME}.Name = {RAW_TABLE_NAME}.Country_Region "
        # "GROUP BY code"

        df = self.spark.read.option("delimiter", ";")\
            .csv(file1, header=True, inferSchema=True)
        df = df.select(df['code'], df['Name'], df['Continent'], df['Region'],
                  df['SurfaceArea'], df['IndepYear'], df['Population'],
                  df['LifeExpectancy'], df['GNP'], df['LocalName'], df['GovernmentForm'],
                  df['HeadOfState'], df['Capital'], df['Code2'])
        end_date = datetime.today()
        latest_df = self.GU.update_latest_data(latest_df, DIM_TABLE_NAME, end_date)


        ####################################
        # Step 3 Write to Database
        ####################################
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print(f'Write to table {DIM_TABLE_NAME}...')
        self.GU.write_to_db(df, DIM_TABLE_NAME, logger)
        print('Done.')

    """
    Dependencies:
        extract_global() ->
        transform_raw_to_dim_country() ->
        transform_raw_to_fact_global() ->
        this
    """

    def aggregate_fact_to_monthly_fact_global(self):
        conn = self.conn
        logger = self.logger
        FACT_TABLE_NAME = 'covid19_global_fact'
        MONTHLY_FACT_TABLE_NAME = 'covid19_global_monthly_fact'

        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE

        print(f'Aggregate data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, MONTHLY_FACT_TABLE_NAME)

        end_date_arr = latest_df.filter(latest_df['table_name'] == FACT_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        fact_df = self.GU.read_from_db(self.spark, FACT_TABLE_NAME)

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming transform
                before = fact_df.count()
                fact_df = fact_df.filter(
                    fact_df['date'] > latest_date
                )
                after = fact_df.count()
                print(f'Skipped {(before - after)} rows')

        #########
        ### Step 2 Update latest date
        #########

        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)
        #########
        ### Step 3 Transform
        #########
        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)
        s = "SELECT dateid, country_code, YEAR(date), MONTH(date), SUM(confirmed) , SUM(deaths) " + \
            f"FROM {FACT_TABLE_NAME} " + \
            "GROUP BY dateid, country_code, YEAR(date), MONTH(date) " + \
            "ORDER BY dateid, country_code, YEAR(date), MONTH(date) "
        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        df = df.toDF('dateid', 'country_code', 'year', 'month', 'confirmed', 'deaths')
        first_day_udf = udf(lambda y, m: datetime(y, m, 1), DateType())
        month_name_udf = udf(lambda d: get_month_name(d), StringType())
        df = df.withColumn('date', first_day_udf(df['year'], df['month']))
        df = df.withColumn('month_name', month_name_udf(df['date']))

        # Reorder the columns to match the schema
        df = df.select(df['dateid'], df['country_code'], df['date'], df['year'],
                       df['month'], df['month_name'], df['confirmed'], df['deaths'])

        #df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')


    """
    Dependencies:
        extract_global() -> transform_raw_to_dim_country() ->this

    src table (Province_State, Country_Region, Lat, Long_,date, confirmed, deaths)
    dest table (dateid, country_code, date, confirmed, deaths)
    """

    def transform_raw_to_fact_global(self):

        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'
        FACT_TABLE_NAME = 'covid19_global_fact'
        DIM_TABLE_NAME = 'country_dim'
        is_resume_extract = False

        latest_date = self.GU.START_DEFAULT_DATE
        end_date = self.GU.START_DEFAULT_DATE
        print(f'Transform data by joining {RAW_TABLE_NAME}, {DIM_TABLE_NAME} to {FACT_TABLE_NAME}.')

        # 1. Transform from raw to fact table

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        end_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        raw_df = self.GU.read_from_db(self.spark, RAW_TABLE_NAME)
        dim_df = self.GU.read_from_db(self.spark, DIM_TABLE_NAME)

        raw_df.createOrReplaceTempView(RAW_TABLE_NAME)
        dim_df.createOrReplaceTempView(DIM_TABLE_NAME)

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming transform
                before = raw_df.count()
                raw_df = raw_df.filter(
                    raw_df['date'] > latest_date
                )
                after = raw_df.count()
                print(f'Skipped {(after - before)} rows')

        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        # Join RAW_TABLE_NAME and COUNTRY_TABLE_NAME
        # s = text("SELECT DISTINCT code, date, SUM(confirmed), SUM(deaths) "
        #          f"FROM  {COUNTRY_TABLE_NAME}, {RAW_TABLE_NAME} "
        #          f"WHERE {COUNTRY_TABLE_NAME}.Name = {RAW_TABLE_NAME}.Country_Region "
        #          "GROUP BY Country_Region, date "
        #          "ORDER BY code "
        #          )

        s = "SELECT DISTINCT Country_Region, date, SUM(confirmed), SUM(deaths) " +\
                 f"FROM  {RAW_TABLE_NAME} " +\
                 "GROUP BY Country_Region, date " +\
                 "ORDER BY date, Country_Region"
        trans_df = self.spark.sql(s)
        trans_df = trans_df.toDF('Country_Region', 'date', 'confirmed', 'deaths')

        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        trans_df = trans_df.withColumn('dateid', dateid_udf(trans_df['date']))

        temp_table_name = RAW_TABLE_NAME+"_trans"
        trans_df.createOrReplaceTempView(temp_table_name)
        s = "SELECT DISTINCT dateid, code as country_code, date, confirmed, deaths " + \
            f"FROM  {temp_table_name} ,{DIM_TABLE_NAME} " + \
            f"WHERE {temp_table_name}.Country_Region = {DIM_TABLE_NAME}.Name " +\
            "ORDER BY dateid, country_code, date"
        df = self.spark.sql(s)
        #df.show(n=50)
        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')


#GU = GlobalUtil.instance()

# spark = SparkSession \
#     .builder \
#     .appName("sb-miniproject6") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
# covid = Covid(spark)
# covid.extract_us()
# covid.transform_raw_to_fact_us()
#covid.aggregate_fact_to_monthly_fact_us()

#covid.transform_raw_to_dim_country()
#covid.extract_global()
#covid.transform_raw_to_fact_global()
#covid.aggregate_fact_to_monthly_fact_global()
