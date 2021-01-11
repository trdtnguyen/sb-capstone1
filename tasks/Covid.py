"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

# import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkFiles  # for reading csv file from https
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when

from db.DB import DB
import configparser
from datetime import timedelta, datetime
import os
from os import environ as env
import pymysql
from sqlalchemy.sql import text

LATEST_DATA_TABLE_NAME = 'latest_data'
START_DEFAULT_DATE = datetime(1990, 1, 1)

project_path = env.get('COVID_PROJECT_PATH')
config = configparser.ConfigParser()
# config.read('/root/airflow/config.cnf')
config_path = os.path.join(project_path, 'config.cnf')
config.read(config_path)

# use rewriteBatchedStatements=true for speed up writing to db
JDBC_MYSQL_URL = 'jdbc:mysql://192.168.0.2:' + \
                 config['DATABASE']['MYSQL_PORT'] + '/' + \
                 config['DATABASE']['MYSQL_DATABASE'] + '?' + \
                 'rewriteBatchedStatements=true'

"""
10/18/20 -> 2020-10-18
"""

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
Transpose from columns with the same type to rows
df: the input dataframe
by_cols: array of columns to be transposed to array. All columns type must have the same type
alias_key: name of new column that represent for all columns in by_cols
alias_val: name of new column that show values of columns in by_cols 
"""


def transpose_columns_to_rows(df, by_cols, alias_key: str, alias_val: str):
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(
        *((col_name, type_name)
          for (col_name, type_name) in df.dtypes if col_name not in by_cols
          )
    )
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"
    kvs = explode(array([
        struct(lit(c).alias(alias_key), col(c).alias(alias_val)) for c in cols
    ])).alias("kvs")
    kvs_key = "kvs." + alias_key
    kvs_val = "kvs." + alias_val

    return df.select(by_cols + [kvs]).select(by_cols + [kvs_key, kvs_val])


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
        url1 = config['COVID19']['COVID19_CONFIRMED_US_URL']
        # confirmed_us_df = pd.read_csv(url1)
        file_name1 = os.path.basename(url1)

        # Unlike pandas, read csv file from https in Spark require more steps
        spark.sparkContext.addFile(url1)
        # confirmed_us_df = self.spark.read.csv(url1, header=True, inferSchema=True) # This will not work
        confirmed_us_df = self.spark.read.csv('file://' + SparkFiles.get(file_name1), header=True, inferSchema=True)
        ncols1 = len(confirmed_us_df.columns)
        # confirmed_us_df.describe().show()

        url2 = config['COVID19']['COVID19_DEATH_US_URL']
        file_name2 = os.path.basename(url2)
        # death_us_df = pd.read_csv(url2)
        spark.sparkContext.addFile(url2)
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
        driver_name = 'com.mysql.cj.jdbc.Driver'
        print('Read from database ...')
        latest_df = spark.read.format('jdbc').options(
            url=JDBC_MYSQL_URL,
            driver=driver_name,
            dbtable=LATEST_DATA_TABLE_NAME,
            user=config['DATABASE']['MYSQL_USER'],
            password=config['DATABASE']['MYSQL_PASSWORD']).load()
        # below code make Spark actually load data
        latest_df = latest_df.cache()
        latest_df.count()

        if len(latest_df.collect()) > 0:
            latest_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
            if len(latest_date_arr) > 0:
                assert len(latest_date_arr) == 1

                latest_date = latest_date_arr[0][1]
                if latest_date > START_DEFAULT_DATE:
                    is_resume_extract = True

        if is_resume_extract:
            if latest_date >= end_date:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                ### Resuming extract
                start_index = date_col + (latest_date.day - begin_date.day)
                confirmed_us_df = confirmed_us_df.select(
                    confirmed_us_df['UID'], confirmed_us_df['iso2'], confirmed_us_df['iso3'],
                    confirmed_us_df['code3'], confirmed_us_df['FIPS'], confirmed_us_df['Admin2'],
                    confirmed_us_df['Province_State'], confirmed_us_df['Country_Region'],
                    confirmed_us_df['Lat'], confirmed_us_df['Long_'],
                    confirmed_us_df['Combined_Key'],
                    confirmed_us_df[(start_index-1):]
                )
                death_us_df = death_us_df.select(
                    death_us_df['UID'], death_us_df['iso2'], death_us_df['iso3'],
                    death_us_df['code3'], death_us_df['FIPS'], death_us_df['Admin2'],
                    death_us_df['Province_State'], death_us_df['Country_Region'],
                    death_us_df['Lat'], death_us_df['Long_'],
                    death_us_df['Combined_Key'], death_us_df['Population'],
                    death_us_df[start_index:]
                )

        #########
        ### Update latest date
        #########

        latest_df = latest_df.withColumn(
            "latest_date",
            when(
                latest_df["table_name"] == RAW_TABLE_NAME,
                end_date
            ).otherwise(latest_df["latest_date"])
        )

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

        trans_df1 = transpose_columns_to_rows(confirmed_us_df,
                                                  by_cols1, 'date', 'confirmed')
        trans_df2 = transpose_columns_to_rows(death_us_df,
                                                  by_cols2, 'date', 'deaths')
        df = trans_df2.join(trans_df1, (trans_df1.UID == trans_df2.UID) & (trans_df1.date == trans_df2.date))\
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
        if __debug__:
            df.show()

        ####################################
        # Step 4 Write to Database
        ####################################
        print('Write to database ...')

        try:
            # overwrite the content of LATEST_DATA_TABLE_NAME
            latest_df.write.format('jdbc').options(
                truncate=True,
                url=JDBC_MYSQL_URL,
                driver=driver_name,
                dbtable=LATEST_DATA_TABLE_NAME,
                user=config['DATABASE']['MYSQL_USER'],
                password=config['DATABASE']['MYSQL_PASSWORD']).mode('overwrite').save()

            if not is_resume_extract:
                dim_df.write.format('jdbc').options(
                    url=JDBC_MYSQL_URL,
                    driver=driver_name,
                    dbtable=DIM_TABLE_NAME,
                    user=config['DATABASE']['MYSQL_USER'],
                    password=config['DATABASE']['MYSQL_PASSWORD']).mode('append').save()
            df.write.format('jdbc').options(
                url=JDBC_MYSQL_URL,
                driver=driver_name,
                dbtable=RAW_TABLE_NAME,
                user=config['DATABASE']['MYSQL_USER'],
                password=config['DATABASE']['MYSQL_PASSWORD']).mode('append').save()
        except ValueError:
            logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')
        print('Done.')

    def extract_global(self):
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'

        is_resume_extract = False
        date_col = 4  # the index of date column in the df

        #######################################
        # Step 1 Read CSV file from datasource to Spark DataFrame
        #######################################
        url1 = config['COVID19']['COVID19_CONFIRMED_GLOBAL_URL']
        file_name1 = os.path.basename(url1)
        spark.sparkContext.addFile(url1)
        confirmed_df = self.spark.read.csv('file://' + SparkFiles.get(file_name1), header=True, inferSchema=True)

        url2 = config['COVID19']['COVID19_DEATH_GLOBAL_URL']
        file_name2 = os.path.basename(url2)
        spark.sparkContext.addFile(url2)
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

        # driver_name = 'com.mysql.jdbc.Driver' # Old driver
        driver_name = 'com.mysql.cj.jdbc.Driver'
        print('Read from database ...')
        latest_df = spark.read.format('jdbc').options(
            url=JDBC_MYSQL_URL,
            driver=driver_name,
            dbtable=LATEST_DATA_TABLE_NAME,
            user=config['DATABASE']['MYSQL_USER'],
            password=config['DATABASE']['MYSQL_PASSWORD']).load()
        # below code make Spark actually load data
        latest_df = latest_df.cache()
        latest_df.count()

        if len(latest_df.collect()) > 0:
            latest_date_arr = latest_df.filter(latest_df['table_name'] == RAW_TABLE_NAME).collect()
            if len(latest_date_arr) > 0:
                assert len(latest_date_arr) == 1

                latest_date = latest_date_arr[0][1]
                if latest_date > START_DEFAULT_DATE:
                    is_resume_extract = True

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
                    confirmed_df[start_index:]
                )
                death_df = death_df.select(
                    death_df['Province/State'], death_df['Country/Region'],
                    death_df['Lat'], death_df['Long_'],
                    death_df[start_index:]
                )

        #########
        ### Update latest date
        #########
        latest_df = latest_df.withColumn(
            "latest_date",
            when(
                latest_df["table_name"] == RAW_TABLE_NAME,
                end_date
            ).otherwise(latest_df["latest_date"])
        )
        print('Done.')
        #######################################
        # Step 3 Transform data
        #######################################
        by_cols = ['Province/State', 'Country/Region', 'Lat', 'Long']
        confirmed_df.show()
        print()
        death_df.show()
        trans_df1 = transpose_columns_to_rows(confirmed_df,
                                              by_cols, 'date', 'confirmed')
        trans_df2 = transpose_columns_to_rows(death_df,
                                              by_cols, 'date', 'deaths')
        df = trans_df2.join(trans_df1, (trans_df1['Country/Region'] == trans_df2['Country/Region']) & (trans_df1.date == trans_df2.date)) \
            .select(
            trans_df2['Province/State'], trans_df2['Country/Region'],
            trans_df2['Lat'], trans_df2['Long'],
            trans_df2['date'], trans_df1['confirmed'], trans_df2['deaths']
        )
        # Refine the date column from 'yyyy/mm/dd' to 'yyyy-mm-dd'
        date_udf = udf(lambda d: convert_date(d), DateType())
        df = df.withColumn('date', date_udf(df['date']))
        df = df.withColumnRenamed('Province/State', 'Province_State')\
            .withColumnRenamed('Country/Region', 'Country_Region') \
            .withColumnRenamed('Long', 'Long_')

        if __debug__:
            df.show()
        test = df.filter(df['Lat'] == 'null')
        test.show()
        ####################################
        # Step 4 Write to Database
        ####################################
        print('Write to database ...')

        try:
            # overwrite the content of LATEST_DATA_TABLE_NAME
            latest_df.write.format('jdbc').options(
                truncate=True,
                url=JDBC_MYSQL_URL,
                driver=driver_name,
                dbtable=LATEST_DATA_TABLE_NAME,
                user=config['DATABASE']['MYSQL_USER'],
                password=config['DATABASE']['MYSQL_PASSWORD']).mode('overwrite').save()

            df.write.format('jdbc').options(
                url=JDBC_MYSQL_URL,
                driver=driver_name,
                dbtable=RAW_TABLE_NAME,
                user=config['DATABASE']['MYSQL_USER'],
                password=config['DATABASE']['MYSQL_PASSWORD']).mode('append').save()
        except ValueError:
            logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')
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
        print(f'Aggregate data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')
        s = text("SELECT UID, YEAR(date), MONTH(date), MONTHNAME(date), SUM(confirmed) , SUM(deaths) "
                 f"FROM {FACT_TABLE_NAME} "
                 "GROUP BY UID, YEAR(date), MONTH(date), MONTHNAME(date) "
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            # transform the datetime to dateid
            insert_list = []
            for row in ret_list:
                insert_val = {}

                year = row[1]
                month = row[2]
                insert_date = datetime(year, month, 1)
                date_str = insert_date.strftime('%Y-%m-%d')

                dateid = int(date_str.replace('-', ''))
                insert_val['dateid'] = dateid
                insert_val['UID'] = row[0]
                insert_val['date'] = insert_date
                insert_val['year'] = year
                insert_val['month'] = month
                insert_val['month_name'] = row[3]
                insert_val['confirmed'] = row[4]
                insert_val['deaths'] = row[5]

                insert_list.append(insert_val)

            df = pd.DataFrame(insert_list)
            if len(df) > 0:
                df.to_sql(MONTHLY_FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}')
            print(e)
        print('Done.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_fact_us() ->
        this
    """

    def transform_raw_to_fact_us(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_us_raw'
        FACT_TABLE_NAME = 'covid19_us_fact'

        # 1. Transform from raw to fact table
        print(f'Transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}.')
        s = text("SELECT UID, date, confirmed, deaths "
                 f"FROM {RAW_TABLE_NAME} "
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            # transform the datetime to dateid
            insert_list = []
            for row in ret_list:
                insert_val = {}

                date = row[1]
                date_str = date.strftime('%Y-%m-%d')
                date_str = date_str.replace('-', '')
                dateid = int(date_str)
                insert_val['dateid'] = dateid
                insert_val['UID'] = row[0]
                insert_val['date'] = row[1]
                insert_val['confirmed'] = row[2]
                insert_val['deaths'] = row[3]

                insert_list.append(insert_val)

            df = pd.DataFrame(insert_list)
            if len(df) > 0:
                df.to_sql(FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}')
            print(e)
        print('Done.')

    """
    Dependencies:
        extract_us() ->
        transform_raw_to_dim_us() (this)

    """

    def transform_raw_to_dim_us(self, ):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_us_raw'
        DIM_TABLE_NAME = 'covid19_us_dim'

        # 1. Transform from raw to dim table
        print(f'Transform data from {RAW_TABLE_NAME} to {DIM_TABLE_NAME}.')
        s = text("SELECT DISTINCT UID, iso2, iso3, code3, FIPS, Admin2, Province_State, "
                 "Country_Region, Lat, Long_, Combined_Key, Population "
                 f"FROM {RAW_TABLE_NAME} "
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            df = pd.DataFrame(ret_list)
            # Refine the column names
            df.columns = keys
            if len(df) > 0:
                df.to_sql(DIM_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {DIM_TABLE_NAME}')
            print(e)
        print('Done.')

    """
    Dependencies:
        extract_global() ->
        transform_raw_to_dim_country() ->
    """

    def transform_raw_to_dim_country(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'
        DIM_TABLE_NAME = 'country_dim'
        COUNTRY_TABLE_NAME = 'world.country'

        # 1. Transform from raw to fact table
        print(f'Transform data from {RAW_TABLE_NAME} and {COUNTRY_TABLE_NAME} to {DIM_TABLE_NAME}.')
        s = text("SELECT DISTINCT code, Name, Lat, Long_, Continent, "
                 "Region, SurfaceArea, IndepYear, Population, "
                 "LifeExpectancy, GNP, LocalName, GovernmentForm, "
                 "HeadOfState, Capital, Code2 "
                 f"FROM {COUNTRY_TABLE_NAME}, {RAW_TABLE_NAME} "
                 f"WHERE {COUNTRY_TABLE_NAME}.Name = {RAW_TABLE_NAME}.Country_Region "
                 "GROUP BY code"
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            df = pd.DataFrame(ret_list)
            # Refine the column names
            df.columns = keys
            if len(df) > 0:
                df.to_sql(DIM_TABLE_NAME, conn, schema=None, if_exists='append', index=False)

        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {DIM_TABLE_NAME}')
            print(e)
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
        print(f'Aggregate data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')
        s = text("SELECT country_code, YEAR(date), MONTH(date), MONTHNAME(date), SUM(confirmed) , SUM(deaths) "
                 f"FROM {FACT_TABLE_NAME} "
                 "GROUP BY country_code, YEAR(date), MONTH(date), MONTHNAME(date) "
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            # transform the datetime to dateid
            insert_list = []
            for row in ret_list:
                insert_val = {}

                year = row[1]
                month = row[2]
                insert_date = datetime(year, month, 1)
                date_str = insert_date.strftime('%Y-%m-%d')

                dateid = int(date_str.replace('-', ''))
                insert_val['dateid'] = dateid
                insert_val['country_code'] = row[0]
                insert_val['date'] = insert_date
                insert_val['year'] = year
                insert_val['month'] = month
                insert_val['month_name'] = row[3]
                insert_val['confirmed'] = row[4]
                insert_val['deaths'] = row[5]

                insert_list.append(insert_val)

            df = pd.DataFrame(insert_list)
            if len(df) > 0:
                df.to_sql(MONTHLY_FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}')
            print(e)
        print('Done.')

    """
    Dependencies:
        extract_global() -> transform_raw_to_dim_country() ->this

    src table (Province_State, Country_Region, Lat, Long_,date, confirmed, deaths)
    dest table (dateid, country_code, date, confirmed, deaths)
    """

    def transform_raw_to_fact_global(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'
        FACT_TABLE_NAME = 'covid19_global_fact'
        COUNTRY_TABLE_NAME = 'country_dim'

        # 1. Transform from raw to fact table
        print(f'Transform data by joining {RAW_TABLE_NAME}, {COUNTRY_TABLE_NAME} to {FACT_TABLE_NAME}.')
        s = text("SELECT DISTINCT code, date, SUM(confirmed), SUM(deaths) "
                 f"FROM  {COUNTRY_TABLE_NAME}, {RAW_TABLE_NAME} "
                 f"WHERE {COUNTRY_TABLE_NAME}.Name = {RAW_TABLE_NAME}.Country_Region "
                 "GROUP BY Country_Region, date "
                 "ORDER BY code "
                 )
        try:
            result = conn.execute(s)
            keys = result.keys()
            ret_list = result.fetchall()
            # transform the datetime to dateid
            insert_list = []
            for row in ret_list:
                insert_val = {}

                date = row[1]
                date_str = date.strftime('%Y-%m-%d')
                date_str = date_str.replace('-', '')
                dateid = int(date_str)
                insert_val['dateid'] = dateid
                insert_val['country_code'] = row[0]
                insert_val['date'] = row[1]
                insert_val['confirmed'] = row[2]
                insert_val['deaths'] = row[3]

                insert_list.append(insert_val)

            df = pd.DataFrame(insert_list)
            if len(df) > 0:
                df.to_sql(FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}')
            print(e)
        print('Done.')


spark = SparkSession \
    .builder \
    .appName("sb-miniproject6") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
covid = Covid(spark)
#covid.extract_us()
covid.extract_global()

# extract_us(conn, logger)
# extract_global(conn, logger)

# transform_raw_to_dim_us(conn, logger)
# transform_raw_to_fact_us(conn, logger)
# aggregate_fact_to_monthly_fact_us(conn,logger)

# transform_raw_to_dim_country(conn, logger)
# transform_raw_to_fact_global(conn, logger)
# aggregate_fact_to_monthly_fact_global(conn, logger)
