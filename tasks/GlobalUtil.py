"""
Global Utils and Constant
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import os
from os import environ as env
from datetime import datetime
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when, lag
from pyspark.sql import Window
from pyspark.sql.types import FloatType
import configparser


class GlobalUtil(object):
    # for Singeton usage
    _instance = None

    # Class attributes as global const
    LATEST_DATA_TABLE_NAME = 'latest_data'
    START_DEFAULT_DATE = datetime(1990, 1, 2)

    PROJECT_PATH = env.get('COVID_PROJECT_PATH')
    if PROJECT_PATH is None:
        print("===> COVID_PROJECT_PATH is not set. Get project path from os.path")
        PROJECT_PATH = os.path.join(os.path.dirname(__file__),"..")

    # CONFIG = configparser.ConfigParser()
    CONFIG = configparser.RawConfigParser()  # Use RawConfigParser() to read url with % character
    CONFIG_FILE = 'config.cnf'
    config_path = os.path.join(PROJECT_PATH, CONFIG_FILE)
    CONFIG.read(config_path)

    JDBC_MYSQL_URL = 'jdbc:mysql://' + CONFIG['DATABASE']['MYSQL_HOST'] + ':' + \
                     CONFIG['DATABASE']['MYSQL_PORT'] + '/' + \
                     CONFIG['DATABASE']['MYSQL_DATABASE'] + '?' + \
                     'rewriteBatchedStatements=true'
    DRIVER_NAME = 'com.mysql.cj.jdbc.Driver'

    def __init__(self):
        raise RuntimeError('Call instance() instead')

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            # Init

        return cls._instance

    """
    Read latest data
    """

    @classmethod
    def read_latest_data(cls, spark, in_table_name):
        is_resume_extract = False
        latest_date = cls.START_DEFAULT_DATE
        try:
            latest_df = spark.read.format('jdbc').options(
                url=cls.JDBC_MYSQL_URL,
                driver=cls.DRIVER_NAME,
                dbtable=cls.LATEST_DATA_TABLE_NAME,
                user=cls.CONFIG['DATABASE']['MYSQL_USER'],
                password=cls.CONFIG['DATABASE']['MYSQL_PASSWORD']).load()
            # below code make Spark actually load data
            latest_df = latest_df.cache()
            latest_df.count()

            if len(latest_df.collect()) > 0:
                latest_date_arr = latest_df.filter(latest_df['table_name'] == in_table_name).collect()
                if len(latest_date_arr) > 0:
                    assert len(latest_date_arr) == 1

                    latest_date = latest_date_arr[0][1]
                    if latest_date > cls.START_DEFAULT_DATE:
                        is_resume_extract = True
            return latest_df, is_resume_extract, latest_date
        except:
            return None, None, None



    """
    Update latest data for a table_name
    """

    @classmethod
    def update_latest_data(cls, latest_df, in_table_name, end_date):
        latest_df = latest_df.withColumn(
            "latest_date",
            when(
                latest_df["table_name"] == in_table_name,
                end_date
            ).otherwise(latest_df["latest_date"])
        )
        return latest_df

    @classmethod
    def write_latest_data(cls, latest_df, logger):
        # overwrite the content of LATEST_DATA_TABLE_NAME
        try:
            latest_df.write.format('jdbc').options(
                truncate=True,
                url=cls.JDBC_MYSQL_URL,
                driver=cls.DRIVER_NAME,
                dbtable=cls.LATEST_DATA_TABLE_NAME,
                user=cls.CONFIG['DATABASE']['MYSQL_USER'],
                password=cls.CONFIG['DATABASE']['MYSQL_PASSWORD']).mode('overwrite').save()
        except ValueError:
            logger.error(f'Error Query when extracting data for {cls.LATEST_DATA_TABLE_NAME} table')

    @classmethod
    def read_from_db(cls, spark, in_table_name):
        df = spark.read.format('jdbc').options(
            url=cls.JDBC_MYSQL_URL,
            driver=cls.DRIVER_NAME,
            dbtable=in_table_name,
            user=cls.CONFIG['DATABASE']['MYSQL_USER'],
            password=cls.CONFIG['DATABASE']['MYSQL_PASSWORD']).load()
        return df

    @classmethod
    def write_to_db(cls, df, table_name, logger, write_mode ='append'):
        try:
            df.write.format('jdbc').options(
                url=cls.JDBC_MYSQL_URL,
                driver=cls.DRIVER_NAME,
                dbtable=table_name,
                user=cls.CONFIG['DATABASE']['MYSQL_USER'],
                password=cls.CONFIG['DATABASE']['MYSQL_PASSWORD']).mode(write_mode).save()
        except ValueError:
            logger.error(f'Error Query when extracting data for {table_name} table')

    """
    Transpose from columns with the same type to rows
    df: the input dataframe
    by_cols: array of columns to be transposed to array. All columns type must have the same type
    alias_key: name of new column that represent for all columns in by_cols
    alias_val: name of new column that show values of columns in by_cols
    """

    @classmethod
    def transpose_columns_to_rows(cls, df, by_cols, alias_key: str, alias_val: str):
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

    """
    Covert from rows selected by col_name to array
    """

    @classmethod
    def rows_to_array(cls, df, col_name: str):
        rdd = df.select(df[col_name]).rdd
        arr = rdd.flatMap(lambda row: row).collect()
        return arr

    """
    Add columns col_name_inc and col_name_inc_pct that is the differences amount and
    different percentage between current row and a previous row
    """
    @classmethod
    def add_prev_diff(cls, df,
                      col_name: str, col_name_inc: str, col_name_inc_pct: str,
                      partition_by_col: str, order_by_col: str):
        if partition_by_col is not None:
            df = df.withColumn('tem_prev', lag(col(col_name))
                               .over(Window.partitionBy(partition_by_col).orderBy(order_by_col)))
        else:
            df = df.withColumn('tem_prev', lag(col(col_name))
                               .over(Window.orderBy(order_by_col)))
        # Fill null with 0
        df = df.fillna({'tem_prev':'0'})

        df = df.withColumn(col_name_inc, col(col_name) - col('tem_prev'))\
            .withColumn(col_name_inc_pct, (col(col_name) - col('tem_prev')) * 100.0 / col('tem_prev'))
        df = df.fillna({col_name_inc_pct: '0'})

        df = df.drop(col('tem_prev'))
        return df
