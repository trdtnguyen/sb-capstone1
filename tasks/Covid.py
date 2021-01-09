"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import pandas as pd
from pyspark.sql import SparkSession

from db.DB import DB
import configparser
from datetime import timedelta, datetime
import os
from os import environ as env
import pymysql
from sqlalchemy.sql import text

project_path = env.get('COVID_PROJECT_PATH')
print(project_path)
# confirmed_cases_US_url = env.get('COVID19_CONFIRMDED_US_URL')
# death_US_url = env.get('COVID19_DEATH_US_URL')
#
# confirmed_case_global_url = env.get('COVID19_CONFIRMED_GLOBAL_URL')
# death_global_url = env.get('COVID19_DEATH_GLOBAL_URL')

"""
10/18/20 -> 2020-10-18
"""


def convert_date(in_date):
    d_list = in_date.split('/')
    year = int('20' + d_list[2])
    month = int(d_list[0])
    day = int(d_list[1])
    # out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]
    out_date = datetime(year, month, day)
    return out_date

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
        print ('Connect string in constructor: ', str_conn)
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

        config = configparser.ConfigParser()
        #config.read('/root/airflow/config.cnf')
        config_path = os.path.join(project_path, 'config.cnf')
        config.read(config_path)
        with open(config_path, 'r') as f:
            test = f.readlines
            print(test)
        url1 = config['COVID19']['COVID19_CONFIRMED_US_URL']
        #confirmed_us_df = pd.read_csv(url1)
        confirmed_us_df = self.spark.read.csv(url1, header=True, inferSchema=True)

        url2 = config['COVID19']['COVID19_DEATH_US_URL']
        #death_us_df = pd.read_csv(url2)
        death_us_df = self.spark.read.csv(url2, header=True, inferSchema=True)

        total_cols = confirmed_us_df.shape[1]
        total_rows = confirmed_us_df.shape[0]
        print(f'Data source Dataframe has {total_rows} rows and {total_cols} cols')

        COL_NAMES = ['UID', 'iso2', 'iso3', 'code3', 'FIPS',
                     'Admin2', 'Province_State', 'Country_Region',
                     'Lat', 'Long_', 'Combined_Key', 'Population',
                     'date', 'confirmed', 'deaths']
        date_col = 12  # the index of date column

        insert_list = []
        dim_insert_list = []

        # for i in range(total_rows):
        # for i, row in confirmed_us_df.iterrows():
        print('Extract confirms values from confirmed_us_df ...', end=' ')
        confirm_2d = []

        for i, row in confirmed_us_df.iterrows():
            # Note that confirmed_us_df has less then death_us_df one column i.e., Population.
            # So we subtract 1 to get the correct column
            confirm_2d.append([row[j] for j in range(date_col - 1, confirmed_us_df.shape[1])])
        print('Done.')

        print('Extract remain info from death_us_df ...', end=' ')
        pct = 0
        for i, row in death_us_df.iterrows():
            pct = i * 100 / total_rows
            if pct % 10 == 0:
                print(f'{pct} ')

            # build the dictionary with key : value is column_name : value
            insert_val = {}
            insert_val['UID'] = int(row['UID'])
            insert_val['iso2'] = row['iso2']
            insert_val['iso3'] = row['iso3']
            insert_val['code3'] = int(row['code3'])
            insert_val['FIPS'] = float(row['FIPS'])
            insert_val['Admin2'] = row['Admin2']
            insert_val['Province_State'] = row['Province_State']
            insert_val['Country_Region'] = row['Country_Region']
            insert_val['Lat'] = float(row['Lat'])
            insert_val['Long_'] = float(row['Long_'])
            insert_val['Combined_Key'] = row['Combined_Key']
            insert_val['Population'] = row['Population']

            copy_insert_val = insert_val.copy()
            dim_insert_list.append(copy_insert_val)

            # Get date columns from date_col to the last column
            for j in range(date_col, death_us_df.shape[1]):
                insert_val2 = insert_val.copy() #reset the insert_val2 using shallow copy
                date = death_us_df.columns[j]
                date = convert_date(date)
                death_num = row[j]
                # confirm_num = confirmed_us_df.iloc[i][j-1] # this is slow
                confirm_num = confirm_2d[i][j - date_col]
                insert_val2['date'] = date
                insert_val2['confirmed'] = int(confirm_num)
                insert_val2['deaths'] = int(death_num)

                insert_list.append(insert_val2)

        print("Extract data Done.")
        print(f'dim_insert_list len: {len(dim_insert_list)}')
        print(f'insert_list len: {len(insert_list)}')

        print(f"Insert to database for table {DIM_TABLE_NAME}...", end=' ')
        df = pd.DataFrame(dim_insert_list)
        try:
            df.to_sql(DIM_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except ValueError:
            logger.error(f'Error Query when extracting data for {DIM_TABLE_NAME} table')
        print('Done.')

        df = pd.DataFrame(insert_list)

        print(f"Insert to database for table {RAW_TABLE_NAME}...", end=' ')
        try:
            df.to_sql(RAW_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
            # manually insert a dictionary will not work due to the limitation number of records insert
            # results = conn.execute(table.insert(), insert_list)
        except ValueError:
            logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')
        print('Done.')


    def extract_global(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'covid19_global_raw'

        # Read CSV files from data sources
        confirmed_global_df = pd.read_csv(env.get('COVID19_CONFIRMED_GLOBAL_URL'))
        death_global_df = pd.read_csv(env.get('COVID19_DEATH_GLOBAL_URL'))

        total_cols = confirmed_global_df.shape[1]
        total_rows = confirmed_global_df.shape[0]
        logger.debug(f'Data source Dataframe has {total_rows} rows and {total_cols} cols')

        COL_NAMES = ['Province_State', 'Country_Region', 'Lat', 'Long_'
                                                                'date', 'confirmed', 'deaths']
        date_col = 4  # the index of date column in the df
        insert_list = []
        logger.info("Extract data from data sources ...")
        logger.info('Extract confirms values from confirmed_global_df ...', end=' ')
        print("Extract data from data sources ...")
        print('Extract confirms values from confirmed_global_df ...', end=' ')
        confirm_2d = []
        for i, row in confirmed_global_df.iterrows():
            confirm_2d.append([row[j] for j in range(date_col, confirmed_global_df.shape[1])])
        print('Done.')
        print('Extract remain info from death_global_df ...', end=' ')
        pct = 0
        for i, row in death_global_df.iterrows():
            pct = i * 100 / total_rows
            if pct % 10 == 0:
                print(f'{pct} ')

            # build the dictionary with key : value is column_name : value
            insert_val = {}
            insert_val['Province_State'] = row['Province/State']
            insert_val['Country_Region'] = row['Country/Region']
            insert_val['Lat'] = float(row['Lat'])
            insert_val['Long_'] = float(row['Long'])

            # Get date columns from date_col to the last column
            for j in range(date_col, death_global_df.shape[1]):
                insert_val2 = insert_val.copy() #reset insert_val2
                date = death_global_df.columns[j]
                date = convert_date(date)
                death_num = row[j]
                # confirm_num = confirmed_us_df.iloc[i][j-1] # this is slow
                confirm_num = confirm_2d[i][j - date_col]
                insert_val2['date'] = date
                insert_val2['confirmed'] = int(confirm_num)
                insert_val2['deaths'] = int(death_num)

                insert_list.append(insert_val2)

        df = pd.DataFrame(insert_list)

        print("Extract data Done.")
        print(f'insert_list len: {len(insert_list)}')
        print("Insert to database...", end=' ')
        # insert database
        # to_sql(name, con, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)[source]
        try:
            df.to_sql(RAW_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
            # manually insert a dictionary will not work due to the limitation number of records insert
            # results = conn.execute(table.insert(), insert_list)
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


    def transform_raw_to_dim_us(self,):
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
covid.extract_us()

#extract_us(conn, logger)
#extract_global(conn, logger)

#transform_raw_to_dim_us(conn, logger)
# transform_raw_to_fact_us(conn, logger)
#aggregate_fact_to_monthly_fact_us(conn,logger)

#transform_raw_to_dim_country(conn, logger)
#transform_raw_to_fact_global(conn, logger)
#aggregate_fact_to_monthly_fact_global(conn, logger)
