"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import os
from os import environ as env
import sys
# sys.path.append(os.path.join(os.path.dirname(__file__), ".", ".."))

from tasks.GlobalUtil import GlobalUtil
# import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkFiles  # for reading csv file from https
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when, expr, lag
from pyspark.sql.window import Window

from db.DB import DB
import configparser
from datetime import timedelta, datetime

print(sys.path)


def convert_date(in_date: str):
    """Convert from string 'yyyy/mm/dd' to datetime
       Args:
           in_date (str): The date string need to be converted to
       Returns:
           datetime: the converted datetime
    """
    d_list = in_date.split('/')
    year = int('20' + d_list[2])
    month = int(d_list[0])
    day = int(d_list[1])
    # out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]
    out_date = datetime(year, month, day)
    return out_date


def from_date_to_dateid(date: datetime):
    """ Convert from datetime to dateid
    Args:
        date (datetime): the input datetime
    Returns:
         int: the dateid converted from date
    """
    date_str = date.strftime('%Y-%m-%d')
    date_str = date_str.replace('-', '')
    dateid = int(date_str)
    return dateid


def create_first_day_of_month(year: int, month: int):
    """ create a datetime object with given year and month and the day as 1
    Args:
        year (int): input year
        month (int): input month
    Returns:
        datetime: the datatime object with given year and month and the day as 1
    """
    return datetime(year, month, 1)


def create_first_dateid_of_month(year: int, month: int):
    """ create a dateid from given year and month with day as 1. This functiton calls from_date_to_dateid() function
    Args:
         year (int): input year
         month (int): input month
    Returns:
        int: dateid
    """
    return from_date_to_dateid(datetime(year, month, 1))


def get_month_name(date):
    """ get month name from input datetime
    Arguments:
         date (datetime): input datetime
    Returns:
        string: month name of the input datetime
    """
    month_name = date.strftime('%B')
    return month_name


class Covid:
    def __init__(self, spark: SparkSession):
        """Constructor
        Arguments:
            spark (SparkSession): spark session creted in the driver
        Returns:
            None
        """
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

    def extract_us(self, end_date=datetime.now()):
        """Extract data from covid19 data source for the US from start_date to end_date.

        This function fill data for covid19_us_raw table and covid19_us_dim table

        start_date gets from default value in config file at the first time.
        From later time, start_date is get from the previous end_date.

        Args:
            end_date (datetime): the end date of the data source

        Returns:
            None

        """
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_RAW_TABLE_NAME']
        DIM_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_DIM_TABLE_NAME']
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

        #######################################
        # Step 2 Read from database to determine the last written data point
        #######################################

        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, RAW_TABLE_NAME)

        # Dealing with dates
        # DEFAULT_START_DATE  < begin_date <= latest_date <= end_date <= max_end_date
        #                         date_col                                   len-1
        # date from death_us_df.schema.names[-1] is the largest end date the data source could have
        begin_date_str = death_us_df.schema.names[date_col]
        begin_date = convert_date(begin_date_str)
        max_end_date_str = death_us_df.schema.names[-1]
        max_end_date = convert_date(max_end_date_str)

        if end_date > max_end_date:
            # we only have what the data set could provide
            end_date = max_end_date
        if latest_date < begin_date:
            latest_date = begin_date

        if latest_date >= end_date:
            print(f'The system has updated data up to {end_date}. No further extract needed.')
            return
        else:
            ### Resuming extract
            diff1 = latest_date - begin_date

            start_index = date_col + (latest_date - begin_date).days + 1
            # note that array[begin:end] excludes the end index, so we need + 1
            end_index = date_col + (end_date - begin_date).days + 1
            confirmed_us_df = confirmed_us_df.select(
                confirmed_us_df['UID'], confirmed_us_df['iso2'], confirmed_us_df['iso3'],
                confirmed_us_df['code3'], confirmed_us_df['FIPS'], confirmed_us_df['Admin2'],
                confirmed_us_df['Province_State'], confirmed_us_df['Country_Region'],
                confirmed_us_df['Lat'], confirmed_us_df['Long_'],
                confirmed_us_df['Combined_Key'],
                *(confirmed_us_df.columns[(start_index - 1):end_index])
            )
            death_us_df = death_us_df.select(
                death_us_df['UID'], death_us_df['iso2'], death_us_df['iso3'],
                death_us_df['code3'], death_us_df['FIPS'], death_us_df['Admin2'],
                death_us_df['Province_State'], death_us_df['Country_Region'],
                death_us_df['Lat'], death_us_df['Long_'],
                death_us_df['Combined_Key'], death_us_df['Population'],
                *(death_us_df.columns[start_index: end_index])
            )

        #########
        ### Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, RAW_TABLE_NAME, end_date)
        # print('Done.')

        #######################################
        # Step 3 Transform data
        #######################################

        # print('Extract remain info from death_us_df ...', end=' ')
        # death_us_df = death_us_df.cache()
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
        # if __debug__:
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
        print('Done extract_us')

    def extract_global(self, end_date=datetime.now()):
        """Extract data from covid19 data source for the global from start_date to end_date.

        This function fill data for covid19_global_raw table and covid19_global_dim table

        start_date gets from default value in config file at the first time.
        From later time, start_date is get from the previous end_date.

        Args:
            end_date (datetime): the end date of the data source

        Returns:
            None

        """
        logger = self.logger
        RAW_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_RAW_TABLE_NAME']
        DIM_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_DIM_TABLE_NAME']
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
        # Filter only data from countries
        confirmed_df = confirmed_df.filter(confirmed_df['Province/State'].isNull())
        confirmed_df = confirmed_df.drop(confirmed_df['Province/State'])

        death_df = death_df.filter(death_df['Province/State'].isNull())
        death_df = death_df.drop(death_df['Province/State'])

        COL_NAMES = ['Province_State', 'Country_Region', 'Lat', 'Long_'
                                                                'date', 'confirmed', 'deaths']
        #######################################
        # Step 2 Read from database to determine the last written data point
        #######################################
        # print('Read from database ...')
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, RAW_TABLE_NAME)
        # latest_df = latest_df.cache()
        # latest_df.count()

        # Get latest date from raw table
        begin_date_str = death_df.schema.names[date_col]
        begin_date = convert_date(begin_date_str)
        max_end_date_str = death_df.schema.names[-1]
        max_end_date = convert_date(max_end_date_str)

        # we only have what the data set could provide
        if end_date > max_end_date:
            end_date = max_end_date
        if latest_date < begin_date:
            latest_date = begin_date

        if latest_date >= end_date:
            print(f'The system has updated data up to {end_date}. No further extract needed.')
            return
        else:
            ### Resuming extract
            start_index = date_col + (latest_date - begin_date).days + 1
            # note that array[begin:end] excludes the end index, so we need + 1
            end_index = date_col + (end_date - begin_date).days + 1
            confirmed_df = confirmed_df.select(
                confirmed_df['Country/Region'],
                confirmed_df['Lat'], confirmed_df['Long'],
                *(confirmed_df.columns[start_index:end_index])
            )
            death_df = death_df.select(
                death_df['Country/Region'],
                death_df['Lat'], death_df['Long'],
                *(death_df.columns[start_index:end_index])
            )

        #########
        ### Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, RAW_TABLE_NAME, end_date)

        #######################################
        # Step 3 Transform data
        #######################################
        # get data for dim table
        if not is_resume_extract:
            dim_df = death_df.select(death_df['Country/Region'], death_df['Lat'], death_df['Long'])

            dim_df = dim_df.withColumnRenamed('Long', 'Long_') \
                .withColumnRenamed('Country/Region', 'Country_Region')
            latest_df = self.GU.update_latest_data(latest_df, DIM_TABLE_NAME, end_date)

        by_cols = ['Country/Region', 'Lat', 'Long']
        trans_df1 = self.GU.transpose_columns_to_rows(confirmed_df,
                                                      by_cols, 'date', 'confirmed')
        trans_df2 = self.GU.transpose_columns_to_rows(death_df,
                                                      by_cols, 'date', 'deaths')
        df = trans_df2.join(trans_df1,
                            (trans_df1['Country/Region'] == trans_df2['Country/Region']) &
                            (trans_df1.date == trans_df2.date)) \
            .select(
            trans_df2['Country/Region'],
            trans_df2['Lat'], trans_df2['Long'],
            trans_df2['date'], trans_df1['confirmed'], trans_df2['deaths']
        )
        # Refine the date column from 'yyyy/mm/dd' to 'yyyy-mm-dd'
        date_udf = udf(lambda d: convert_date(d), DateType())
        df = df.withColumn('date', date_udf(df['date']))
        df = df.withColumnRenamed('Country/Region', 'Country_Region') \
            .withColumnRenamed('Long', 'Long_')

        # filter null
        before = df.count()
        df = df.where(df['Lat'].isNotNull())
        after = before = df.count()
        print(f'filtered out {(before - after)} rows with Lat is NULL')

        ####################################
        # Step 4 Write to Database
        ####################################
        if not is_resume_extract:
            print(f'Write to table {DIM_TABLE_NAME} ...')
            self.GU.write_to_db(dim_df, DIM_TABLE_NAME, logger)

        print(f'Write to table {RAW_TABLE_NAME}...')
        self.GU.write_to_db(df, RAW_TABLE_NAME, logger)

        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done extract_global.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_fact_us() ->
        aggregate_fact_to_monthly_fact_us() (this)
    """

    def aggregate_fact_to_monthly_fact_us(self):
        conn = self.conn
        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_FACT_TABLE_NAME']
        MONTHLY_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_MONTHLY_FACT_TABLE_NAME']

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

        ## Note: We need to perfrom "Upsert" operation (Update or Insert) to ensure the last month data is correct
        ### Currently, we don't implement Resuming transform since we still not figure out how to Upsert
        ### So all monthly data is rewrite everytime we run the ETL pipeline
        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming transform
                ### TODO: impelement Upsert in order to use this
                pass
                # before = fact_df.count()
                # fact_df = fact_df.filter(
                #     fact_df['date'] > latest_date
                # )
                # after = fact_df.count()
                # print(f'Skipped {(before - after)} rows')

        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)
        s = "SELECT Province_State, YEAR(date), MONTH(date), MAX(confirmed) , MAX(deaths)" + \
            f"FROM {FACT_TABLE_NAME} " + \
            "GROUP BY Province_State, YEAR(date), MONTH(date)"

        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        df = df.toDF('Province_State', 'year', 'month', 'confirmed', 'deaths')

        # Add column 'dateid' and 'date'
        first_day_udf = udf(lambda y, m: datetime(y, m, 1), DateType())
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())

        # month_name_udf = udf(lambda d: get_month_name(d), StringType())
        month_name_udf = udf(lambda d: d.strftime('%B'), StringType())
        df = df.withColumn('date', first_day_udf(df['year'], df['month']))
        df = df.withColumn('dateid', dateid_udf(df['date'])) \
            .withColumn('month_name', month_name_udf(df['date']))

        # Reorder the columns to match the schema
        # df = df.select(df['dateid'], df['UID'], df['date'], df['year'],
        #                df['month'], df['month_name'], df['confirmed'], df['deaths'])

        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'confirmed', 'confirmed_inc', 'confirmed_inc_pct',
                                   'Province_State', 'date')
        df = self.GU.add_prev_diff(df,
                                   'deaths', 'deaths_inc', 'deaths_inc_pct',
                                   'Province_State', 'date')
        # df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger, 'overwrite')
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_fact_us() ->
        this
    """

    def transform_raw_to_fact_us(self, end_date=datetime.now()):

        logger = self.logger
        RAW_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_RAW_TABLE_NAME']
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_FACT_TABLE_NAME']
        is_resume_extract = False

        latest_date = self.GU.START_DEFAULT_DATE
        # end_date = self.GU.START_DEFAULT_DATE

        print(f'Transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}.')

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        # assign the end_date to the latest day in raw table
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
                raw_df = raw_df.filter(
                    raw_df['date'] > latest_date
                )

        # creating table view should be placed after the filtering date
        raw_df.createOrReplaceTempView(RAW_TABLE_NAME)
        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        # Filtering specific columns and sum up for state
        key1 = 'Province_State'
        key2 = 'date'
        s = f"SELECT {key1}, {key2}, SUM(confirmed) as confirmed, SUM(deaths) as deaths" + \
            f" FROM {RAW_TABLE_NAME} " + \
            f" GROUP BY {key1}, {key2} " + \
            f" ORDER BY {key1}, {key2}"
        df = self.spark.sql(s)
        # Rename
        df = df.select(
            col(key1), col(key2), col('confirmed'), col('deaths')).distinct()
        # Add column "dateid" computed from "date"
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        df = df.withColumn('dateid', dateid_udf(raw_df['date']))

        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'confirmed', 'confirmed_inc', 'confirmed_inc_pct',
                                   key1, key2)
        df = self.GU.add_prev_diff(df,
                                   'deaths', 'deaths_inc', 'deaths_inc_pct',
                                   key1, key2)
        # if __debug__:
        # df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)

        print('Done.')

    """
    Read local CSV file and write on corresponding table in database
    Dependencies:
        None. This function could work independently with others
    """

    def transform_raw_to_dim_country(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_RAW_TABLE_NAME']
        DIM_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_DIM_TABLE_NAME']
        COUNTRY_TABLE_NAME = self.GU.CONFIG['DATABASE']['COUNTRY_TABLE_NAME']

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

        df = self.spark.read.option("delimiter", ";") \
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
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_FACT_TABLE_NAME']
        MONTHLY_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_MONTHLY_FACT_TABLE_NAME']

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
                ### TODO: implement Upsert in order to use this
                pass
                # ### Resuming transform
                # before = fact_df.count()
                # fact_df = fact_df.filter(
                #     fact_df['date'] > latest_date
                # )
                # after = fact_df.count()
                # print(f'Skipped {(before - after)} rows')

        #########
        ### Step 2 Update latest date
        #########

        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)
        #########
        ### Step 3 Transform
        #########
        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)
        key = 'Country_Region'
        s = f"SELECT {key}, YEAR(date), MONTH(date), MAX(confirmed) , MAX(deaths) " + \
            f"FROM {FACT_TABLE_NAME} " + \
            f"GROUP BY {key}, YEAR(date), MONTH(date) " + \
            f"ORDER BY {key}, YEAR(date), MONTH(date) "
        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        df = df.toDF(key, 'year', 'month', 'confirmed', 'deaths')
        first_day_udf = udf(lambda y, m: datetime(y, m, 1), DateType())
        first_dateid_udf = udf(lambda y, m: create_first_dateid_of_month(y, m), IntegerType())
        month_name_udf = udf(lambda d: get_month_name(d), StringType())
        df = df.withColumn('dateid', first_dateid_udf(df['year'], df['month']))
        df = df.withColumn('date', first_day_udf(df['year'], df['month']))
        df = df.withColumn('month_name', month_name_udf(df['date']))

        # Reorder the columns to match the schema
        df = df.select(df['dateid'], df[key], df['date'], df['year'],
                       df['month'], df['month_name'], df['confirmed'], df['deaths'])

        # df.show()
        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'confirmed', 'confirmed_inc', 'confirmed_inc_pct',
                                   key, 'date')
        df = self.GU.add_prev_diff(df,
                                   'deaths', 'deaths_inc', 'deaths_inc_pct',
                                   key, 'date')

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        # self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger)
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger, 'overwrite')
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

    def transform_raw_to_fact_global(self, end_date=datetime.now()):
        """ Transform raw table to fact table

        src table (Province_State, Country_Region, Lat, Long_,date, confirmed, deaths)
        dest table (dateid, country_code, date, confirmed, deaths)

        Arguments:
            end_date (datetime): the end date of data
        Dependencies:
            extract_global() -> transform_raw_to_dim_country() ->this

        """

        logger = self.logger
        RAW_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_RAW_TABLE_NAME']
        DIM_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_DIM_TABLE_NAME']
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_FACT_TABLE_NAME']

        is_resume_extract = False

        latest_date = self.GU.START_DEFAULT_DATE
        # end_date = self.GU.START_DEFAULT_DATE
        print(f'Transform data by joining {RAW_TABLE_NAME}, {DIM_TABLE_NAME} to {FACT_TABLE_NAME}.')

        # 1. Transform from raw to fact table

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        # assign the end_date to the latest day in raw table
        end_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        raw_df = self.GU.read_from_db(self.spark, RAW_TABLE_NAME)
        dim_df = self.GU.read_from_db(self.spark, DIM_TABLE_NAME)

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
                print(f'Skipped {(before - after)} rows')

        # creating table view should be placed after the filtering date
        raw_df.createOrReplaceTempView(RAW_TABLE_NAME)
        #########
        ### Step 2 Update latest date
        #########
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #########
        ### Step 3 Transform
        #########
        # Note: some contries (e.g., United Kingdom) have data from both the country and the Province_State
        # so in such case, we just get the data from row where Province_State is null (the country)
        key = 'Country_Region'
        s = f"SELECT DISTINCT {key}, date, confirmed, deaths " + \
            f" FROM  {RAW_TABLE_NAME} " + \
            f"ORDER BY date, {key}"
        trans_df = self.spark.sql(s)
        trans_df = trans_df.toDF(key, 'date', 'confirmed', 'deaths')

        # add dateid column
        dateid_udf = udf(lambda d: from_date_to_dateid(d), IntegerType())
        df = trans_df.withColumn('dateid', dateid_udf(trans_df['date']))
        # TODO: current country_dim table is out-of-date. So we use only the covid19_global_raw
        # without join with country_dim

        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'confirmed', 'confirmed_inc', 'confirmed_inc_pct',
                                   key, 'date')
        df = self.GU.add_prev_diff(df,
                                   'deaths', 'deaths_inc', 'deaths_inc_pct',
                                   key, 'date')
        # df.show(n=50)
        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')



    def aggregate_fact_to_sum_fact(self, end_date=datetime.now()):
        """ Aggregate fact tables from both US tables and global table into a new table by sum aggregate

        fact tables must be populated first before running this function.
        Dependencies:
            extract_us() ->
            transform_raw_to_fact_us().
            extract_global() ->
            transform_raw_to_fact_global() ->
            this
        """

        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_SUM_FACT_TABLE']
        US_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_US_FACT_TABLE_NAME']
        GLOBAL_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_GLOBAL_FACT_TABLE_NAME']
        is_resume_extract = False
        latest_date = self.GU.START_DEFAULT_DATE

        # the covid data only have data from Jan 02 2020
        start_date = datetime(2020, 1, 2)
        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        # assign the end_date to the latest day in raw table
        end_date_arr = latest_df.filter(latest_df['table_name'] == US_FACT_TABLE_NAME).collect()
        if len(end_date_arr) > 0:
            assert len(end_date_arr) == 1
            end_date = end_date_arr[0][1]

        if is_resume_extract:
            # we only compare two dates by day, month, year excluding time
            # if latest_date.day == end_date.day and \
            #         latest_date.month == end_date.month and \
            #         latest_date.year == end_date.year:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                start_date = latest_date

        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)
        #########
        ### Step 2 Read fact tables, filter, and create temp view
        #########
        us_fact_df = self.GU.read_from_db(self.spark, US_FACT_TABLE_NAME)
        us_fact_df = us_fact_df.filter(us_fact_df['date'] > start_date)
        us_fact_df.createOrReplaceTempView(US_FACT_TABLE_NAME)

        global_fact_df = self.GU.read_from_db(self.spark, GLOBAL_FACT_TABLE_NAME)
        global_fact_df = global_fact_df.filter(global_fact_df['date'] > start_date)
        global_fact_df.createOrReplaceTempView(GLOBAL_FACT_TABLE_NAME)
        #########
        ### Step 3 Aggregate each table to compute sum of cases
        #########
        s = "SELECT dateid, date, " + \
            "SUM(confirmed) as us_confirmed, SUM(deaths) as us_deaths " + \
            f" FROM  {US_FACT_TABLE_NAME}" + \
            f" GROUP BY dateid, date " + \
            f" ORDER BY dateid, date"
        tem_us_df = self.spark.sql(s)
        # tem_us_df.show()
        tem_us_df.createOrReplaceTempView('tem1')

        s = "SELECT dateid, date, " + \
            "SUM(confirmed) as global_confirmed, SUM(deaths) as global_deaths " + \
            f" FROM  {GLOBAL_FACT_TABLE_NAME}" + \
            f" GROUP BY dateid, date " + \
            f" ORDER BY dateid, date"
        tem_global_df = self.spark.sql(s)
        # tem_global_df.show()
        tem_global_df.createOrReplaceTempView('tem2')

        #########
        ### Step 4 Join temp view
        #########
        s = "SELECT u.dateid, u.date, " + \
            "us_confirmed, us_deaths, global_confirmed, global_deaths " + \
            f" FROM tem1 as u, tem2 as g" + \
            f" WHERE u.dateid=  g.dateid" + \
            f" ORDER BY u.dateid, u.date"
        df = self.spark.sql(s)

        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'us_confirmed', 'us_confirmed_inc', 'us_confirmed_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'us_deaths', 'us_deaths_inc', 'us_deaths_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'global_confirmed', 'global_confirmed_inc', 'global_confirmed_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'global_deaths', 'global_deaths_inc', 'global_deaths_inc_pct',
                                   None, 'date')

        df = df.select(df['dateid'], df['date'],
                       df['us_confirmed'], df['us_deaths'],
                       df['us_confirmed_inc'], df['us_deaths_inc'],
                       df['us_confirmed_inc_pct'], df['us_deaths_inc_pct'],
                       df['global_confirmed'], df['global_deaths'],
                       df['global_confirmed_inc'], df['global_deaths_inc'],
                       df['global_confirmed_inc_pct'], df['global_deaths_inc_pct']
                       )
        # df.show()
        ####################################
        # Step 5 Write to Database
        ####################################
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')


    def aggregate_fact_to_sum_monthly_fact(self, end_date=datetime.now()):
        """ aggregate from fact table to monthly fact table

        Dependencies:
        aggregate_fact_to_sum_fact

        Returns:
            None
        """
        logger = self.logger
        FACT_TABLE_NAME = 'covid19_sum_fact'
        MONTHLY_FACT_TABLE_NAME = 'covid19_sum_monthly_fact'
        latest_date = self.GU.START_DEFAULT_DATE

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
        # fact_df.show()
        # Note: We need to perform "Upsert" operation (Update or Insert) to ensure the last month data is correct
        # Currently, we don't implement Resuming transform since we still not figure out how to Upsert
        # So all monthly data is rewrite everytime we run the ETL pipeline
        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### TODO: impelement Upsert in order to use this
                pass
                ### Resuming transform
                # before = fact_df.count()
                # fact_df = fact_df.filter(
                #     fact_df['date'] > latest_date
                # )
                # after = fact_df.count()
                # print(f'Skipped {(before - after)} rows')

        # Update latest date
        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)
        #########
        ### Step 2 Transform
        #########
        print(f'Transform data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')

        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)

        s = "SELECT YEAR(date), MONTH(date), " + \
            " MAX(us_confirmed), MAX(us_deaths), " + \
            "MAX(global_confirmed), MAX(global_deaths) " + \
            f"FROM {FACT_TABLE_NAME} " + \
            f"GROUP BY YEAR(date), MONTH(date) " + \
            "ORDER BY YEAR(date), MONTH(date) "

        df = self.spark.sql(s)

        # Change columns name by index to match the schema
        df = df.toDF('year', 'month',
                     'us_confirmed', 'us_deaths', 'global_confirmed', 'global_deaths')
        first_day_udf = udf(lambda y, m: datetime(y, m, 1), DateType())
        first_dateid_udf = udf(lambda y, m: create_first_dateid_of_month(y, m), IntegerType())
        month_name_udf = udf(lambda d: get_month_name(d), StringType())
        df = df.withColumn('dateid', first_dateid_udf(df['year'], df['month']))
        df = df.withColumn('date', first_day_udf(df['year'], df['month']))
        df = df.withColumn('month_name', month_name_udf(df['date']))

        # Aggregate columns that show differences between curernt day and the previous day
        df = self.GU.add_prev_diff(df,
                                   'us_confirmed', 'us_confirmed_inc', 'us_confirmed_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'us_deaths', 'us_deaths_inc', 'us_deaths_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'global_confirmed', 'global_confirmed_inc', 'global_confirmed_inc_pct',
                                   None, 'date')
        df = self.GU.add_prev_diff(df,
                                   'global_deaths', 'global_deaths_inc', 'global_deaths_inc_pct',
                                   None, 'date')
        # Reorder the columns to match the schema
        df = df.select(df['dateid'], df['date'], df['year'], df['month'], df['month_name'],
                       df['us_confirmed'], df['us_deaths'],
                       df['us_confirmed_inc'], df['us_deaths_inc'],
                       df['us_confirmed_inc_pct'], df['us_deaths_inc_pct'],
                       df['global_confirmed'], df['global_deaths'],
                       df['global_confirmed_inc'], df['global_deaths_inc'],
                       df['global_confirmed_inc_pct'], df['global_deaths_inc_pct']
                       )

        # df.show()
        ####################################
        # Step 3 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        # self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger)
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger, 'overwrite')
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

# Self test
# GU = GlobalUtil.instance()

# spark = SparkSession \
#     .builder \
#     .appName("sb-miniproject6") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
# covid = Covid(spark)
# covid.extract_us()
# covid.transform_raw_to_fact_us()
# covid.aggregate_fact_to_monthly_fact_us()
# #
# covid.transform_raw_to_dim_country()
# covid.extract_global()
# covid.transform_raw_to_fact_global()
# covid.aggregate_fact_to_monthly_fact_global()
#
# covid.aggregate_fact_to_sum_fact()
# covid.aggregate_fact_to_sum_monthly_fact()
