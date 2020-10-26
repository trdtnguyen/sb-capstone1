from db.DB import DB
import pandas as pd
import configparser
from sqlalchemy.sql import text

import requests
import json
import prettytable

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
    date = datetime(year, int(str_m),1)
    return date



def extract_BOL(conn, logger,
                start_year=datetime.now().year, end_year=datetime.now().year):
    RAW_TABLE_NAME = 'bol_raw'
    DIM_TABLE_NAME = 'bol_series_dim'
    # 1. Resuming extract data.
    s = text("SELECT year "
             f"FROM {RAW_TABLE_NAME} "
             "ORDER BY year DESC "
             "LIMIT 1")

    try:
        ret_list = conn.execute(s).fetchall()
        df_tem = pd.DataFrame(ret_list)
        if len(df_tem) > 0:
            latest_year = df_tem.iloc[0][0]
            if latest_year > end_year:
                print(f'database last update on {latest_year} '
                      f'that is later than the end date {end_year}. No further extract needed')
                return
            else:
                print(f'set start_year to {latest_year}')
                start_year = latest_year

    except pymysql.OperationalError as e:
        logger.error(f'Error Query when get latest year in {RAW_TABLE_NAME}')
        print(e)

    headers = {'Content-type': 'application/json'}

    # 2. Read series
    print('Read series ...', end=  " ")
    s = text("SELECT series_id "
             f"FROM {DIM_TABLE_NAME} "
             )
        #series_ids = ['CUUR0000SA0', 'SUUR0000SA0']
    try:
        ret_list = conn.execute(s).fetchall()
        df_tem = pd.DataFrame(ret_list)
        if len(df_tem) > 0:
            series_ids_tem = df_tem.values.tolist() # Get array of array
            series_ids = [arr[0] for arr in series_ids_tem]
        else:
            logger.error('The series_ids is empty')
            print('The series_ids is empty')
            return

    except pymysql.OperationalError as e:
        logger.error('Error Query when get latest year in BOL_raw')
        print(e)
    print("Done. Number of series: ", len(series_ids))
    # 3 Extract Data using API
    insert_list = []

    API_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    data = json.dumps({"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)})

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

    df = pd.DataFrame(insert_list)
    print("Extract data Done.")
    print("Insert to database...", end=' ')
    try:
        df.to_sql(RAW_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        # manually insert a dictionary will not work due to the limitation number of records insert
        # results = conn.execute(table.insert(), insert_list)
    except ValueError:
        logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')
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

extract_BOL(conn, logger, 2010, 2020)