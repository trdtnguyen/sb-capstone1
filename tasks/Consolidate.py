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

        s = "SELECT DISTINCT c.dateid, c.date, c.us_confirmed, c.us_deaths, " + \
            "c.global_confirmed, c.global_deaths,  " + \
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


# self test
spark = SparkSession \
    .builder \
    .appName("sb-miniproject6") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

consolid = Consolidation(spark)
consolid.consolidate_covid_stock()
