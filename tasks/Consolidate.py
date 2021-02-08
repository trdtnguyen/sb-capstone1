"""
Consolidate data from tables for data analysis
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from tasks.GlobalUtil import GlobalUtil
from db.DB import DB

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when, last
from pyspark.sql.window import Window
import configparser
from datetime import timedelta, datetime
import sys

def from_date_to_dateid(date: datetime):
    date_str = date.strftime('%Y-%m-%d')
    date_str = date_str.replace('-', '')
    dateid = int(date_str)
    return dateid

def create_first_day_of_month(year: int, month: int):
    return datetime(year, month, 1)

def create_first_dateid_of_month(year: int, month: int):
    return from_date_to_dateid(datetime(year, month, 1))

def get_month_name(date):
    month_name = date.strftime('%B')
    return month_name

class Consolidation:
    def __init__(self, spark: SparkSession):
        self.GU = GlobalUtil.instance()
        self.spark = spark

        user = self.GU.CONFIG['DATABASE']['MYSQL_USER']
        db_name = self.GU.CONFIG['DATABASE']['MYSQL_DATABASE']
        pw = self.GU.CONFIG['DATABASE']['MYSQL_PASSWORD']
        host = self.GU.CONFIG['DATABASE']['MYSQL_HOST']

        config = configparser.ConfigParser()
        # config.read('config.cnf')
        config.read('/root/airflow/config.cnf')
        str_conn = f'mysql+pymysql://{user}:{pw}@{host}/{db_name}'
        self.db = DB(str_conn)
        self.conn = self.db.get_conn()
        self.logger = self.db.get_logger()

    """
    Consolidate Stock and Covid data on daily basic
    """
    def consolidate_covid_stock(self):
        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_STOCK_FACT_TABLE_NAME']
        STOCK_INDEX_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['STOCK_INDEX_FACT_TABLE_NAME']
        COVID_SUM_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_SUM_FACT_TABLE']
        COL_NAMES = ['dowjones_score', 'nasdaq100_score', 'sp500_score']
        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        end_date = datetime.now()

        if is_resume_extract:
            # we only compare two dates by day, month, year excluding time
            if latest_date.day == end_date.day and \
                    latest_date.month == end_date.month and \
                    latest_date.year == end_date.year:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                start_date = latest_date
        # update latest_data table for FACT_TABLE_NAME
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #######################################
        # Step 2 Consolidate data
        #######################################
        # Read tables on main memory for join
        df1 = self.GU.read_from_db(self.spark, STOCK_INDEX_FACT_TABLE_NAME)
        df1.createOrReplaceTempView(STOCK_INDEX_FACT_TABLE_NAME)

        df2 = self.GU.read_from_db(self.spark, COVID_SUM_FACT_TABLE_NAME)
        df2.createOrReplaceTempView(COVID_SUM_FACT_TABLE_NAME)

        s = "SELECT DISTINCT c.dateid, c.date, " + \
            "c.us_confirmed, c.us_deaths, c.us_confirmed_inc, c.us_deaths_inc, " + \
            "c.us_confirmed_inc_pct, c.us_deaths_inc_pct, " + \
            "c.global_confirmed, c.global_deaths, c.global_confirmed_inc, c.global_deaths_inc,  " + \
            "c.global_confirmed_inc_pct, c.global_deaths_inc_pct, " + \
            f"{COL_NAMES[0]}, {COL_NAMES[1]}, {COL_NAMES[2]} " + \
            f"FROM {COVID_SUM_FACT_TABLE_NAME} as c LEFT JOIN" + \
            f" {STOCK_INDEX_FACT_TABLE_NAME} as s ON c.dateid = s.dateid " +\
            "ORDER BY c.dateid, c.date"

        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        # df.show()

        # Stock doesn't have data on weekend, so on those days the stock data are null
        # Filling null values with the previous non-null value
        for i in range(len(COL_NAMES)):
            df = df.withColumn(f'{COL_NAMES[i]}',
                               last(f'{COL_NAMES[i]}', True)
                                       .over(Window.orderBy('dateid')
                                       .rowsBetween(-sys.maxsize, 0)
                               ))
        # df.show()
        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')

    """
    Aggregate from daily data to monthly data
    """
    def aggregate_covid_stock_monthly_fact(self):
        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_STOCK_FACT_TABLE_NAME']
        MONTHLY_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_STOCK_MONTHLY_FACT_TABLE_NAME']

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

        latest_df = self.GU.update_latest_data(latest_df, MONTHLY_FACT_TABLE_NAME, end_date)
        #########
        ### Step 2 Transform
        #########
        print(f'Transform data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')
        fact_df.createOrReplaceTempView(FACT_TABLE_NAME)

        s = "SELECT YEAR(date), MONTH(date), " + \
            "MAX(us_confirmed) , MAX(us_deaths), MAX(global_confirmed) , MAX(global_deaths), " + \
            "AVG(sp500_score) , AVG(nasdaq100_score), AVG(dowjones_score) " + \
            f"FROM {FACT_TABLE_NAME} " + \
            "GROUP BY YEAR(date), MONTH(date) " + \
            "ORDER BY YEAR(date), MONTH(date) "

        df = self.spark.sql(s)
        # Change columns name by index to match the schema
        df = df.toDF('year', 'month',
                     'us_confirmed', 'us_deaths', 'global_confirmed', 'global_deaths',
                     'sp500_score', 'nasdaq100_score', 'dowjones_score')
        # Add dateid, date, mont_date columns
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
                       df['global_confirmed_inc_pct'], df['global_deaths_inc_pct'],
                       df['sp500_score'], df['nasdaq100_score'], df['dowjones_score']
                       )
        # Reorder the columns to match the schema
        # df = df.select(df['dateid'], df['date'], df['year'], df['month'], df['month_name'],
        #                df['us_confirmed'], df['us_deaths'], df['global_confirmed'], df['global_deaths'],
        #                df['sp500_score'], df['nasdaq100_score'], df['dowjones_score']
        #                )

        ####################################
        # Step 4 Write to Database
        ####################################
        print(f'Write to table {MONTHLY_FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, MONTHLY_FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')
    """
    Consolidate Covid, Stock and BOL data from fact tables
    """
    def consolidate_covid_stock_bol(self):
        logger = self.logger
        FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_STOCK_BOL_FACT_TABLE_NAME']
        COVID_STOCK_MONTHLY_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['COVID_STOCK_MONTHLY_FACT_TABLE_NAME']
        BOL_SERIES_FACT_TABLE_NAME = self.GU.CONFIG['DATABASE']['BOL_SERIES_FACT_TABLE_NAME']

        #######################################
        # Step 1 Read from database to determine the last written data point
        #######################################
        latest_df, is_resume_extract, latest_date = \
            self.GU.read_latest_data(self.spark, FACT_TABLE_NAME)

        end_date = datetime.now()

        if is_resume_extract:
            # we only compare two dates by day, month, year excluding time
            if latest_date.day == end_date.day and \
                    latest_date.month == end_date.month and \
                    latest_date.year == end_date.year:
                print(f'The system has updated data up to {end_date}. No further extract needed.')
                return
            else:
                start_date = latest_date
        # update latest_data table for FACT_TABLE_NAME
        latest_df = self.GU.update_latest_data(latest_df, FACT_TABLE_NAME, end_date)

        #######################################
        # Step 2 Consolidate data
        #######################################
        # Read tables on main memory for join
        df1 = self.GU.read_from_db(self.spark, COVID_STOCK_MONTHLY_FACT_TABLE_NAME)
        df1.createOrReplaceTempView(COVID_STOCK_MONTHLY_FACT_TABLE_NAME)

        df2 = self.GU.read_from_db(self.spark, BOL_SERIES_FACT_TABLE_NAME)
        df2.createOrReplaceTempView(BOL_SERIES_FACT_TABLE_NAME)

        s = "SELECT DISTINCT b.dateid, b.date, YEAR(b.date) as year, MONTH(b.date) as month, " + \
            " b.series_id as bol_series_id, b.value as bol_series_value, " + \
            "c.us_confirmed, c.us_deaths, c.us_confirmed_inc, c.us_deaths_inc, " + \
            "c.us_confirmed_inc_pct, c.us_deaths_inc_pct, " + \
            "c.global_confirmed, c.global_deaths, c.global_confirmed_inc, c.global_deaths_inc,  " + \
            "c.global_confirmed_inc_pct, c.global_deaths_inc_pct, " + \
            "c.sp500_score, c.nasdaq100_score, c.dowjones_score " + \
            f" FROM {BOL_SERIES_FACT_TABLE_NAME} as b LEFT JOIN " + \
            f" {COVID_STOCK_MONTHLY_FACT_TABLE_NAME} as c ON b.dateid = c.dateid " + \
            " ORDER BY b.dateid, bol_series_id"

        df = self.spark.sql(s)

        # Since BOL has longer history data, some of columns from COVID_STOCK_MONTHLY_FACT_TABLE_NAME
        # would have null value, we fill those nulls with 0
        df = df.na.fill(value=0)

        month_name_udf = udf(lambda d: d.strftime('%B'), StringType())
        df = df.withColumn('month_name', month_name_udf(df['date']))

        print(f'Write to table {FACT_TABLE_NAME}...')
        self.GU.write_to_db(df, FACT_TABLE_NAME, logger)
        print(f'Write to table {self.GU.LATEST_DATA_TABLE_NAME}...')
        self.GU.write_latest_data(latest_df, logger)
        print('Done.')


# self test
# spark = SparkSession \
#     .builder \
#     .appName("sb-miniproject6") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
# # Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
#
# consolidate = Consolidation(spark)
# consolidate.consolidate_covid_stock()
# consolidate.aggregate_covid_stock_monthly_fact()