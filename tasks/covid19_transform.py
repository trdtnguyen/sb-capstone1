"""
Tranform Covid-19 data from raw table to dim table and fact table
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from db.DB import DB
import pandas as pd
import configparser
from datetime import timedelta, datetime
import pymysql
from sqlalchemy.sql import text


def transform_raw_to_dim_us(conn, logger):
    RAW_TABLE_NAME = 'covid19_us_raw'
    DIM_TABLE_NAME = 'covid19_us_dim'


    # 1. Transfrom from raw to dim table
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
        #Refine the column names
        df.columns = keys
        if len(df) > 0:
            df.to_sql(DIM_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
    except pymysql.OperationalError as e:
        logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {DIM_TABLE_NAME}')
        print(e)
    print('Done.')


config = configparser.ConfigParser()
# config.read('config.cnf')
config.read('../config.cnf')
# str_conn  = 'mysql+pymysql://root:12345678@localhost/bank'
str_conn = 'mysql+pymysql://'
str_conn += config['DATABASE']['user'] + ':' + config['DATABASE']['pw'] + \
            '@' + config['DATABASE']['host'] + '/' + config['DATABASE']['db_name']
print(str_conn)
db = DB(str_conn)

conn = db.get_conn()
logger = db.get_logger()
transform_raw_to_dim_us(conn, logger)