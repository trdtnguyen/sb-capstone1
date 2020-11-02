"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

import pandas as pd
from db.DB import DB
import configparser
from sqlalchemy.exc import DBAPIError
from sqlalchemy import Table, select, update, insert
from datetime import timedelta, datetime


confirmed_cases_US_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
death_US_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'

confirmed_case_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
death_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'

"""
10/18/20 -> 2020-10-18
"""


def convert_date(in_date):
    d_list = in_date.split('/')
    year = int('20' + d_list[2])
    month = int(d_list[0])
    day = int(d_list[1])
    #out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]
    out_date = datetime(year, month, day)


    return out_date


def extract_us(conn, logger):
    RAW_TABLE_NAME = 'covid19_us_raw'
    DIM_TABLE_NAME = 'covid19_us_dim'

    print("Extract data from data sources ...")
    confirmed_us_df = pd.read_csv(confirmed_cases_US_url)
    death_us_df = pd.read_csv(death_US_url)

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


def extract_global(conn, logger):
    RAW_TABLE_NAME = 'covid19_global_raw'
    confirmed_global_df = pd.read_csv(confirmed_case_global_url)
    death_global_df = pd.read_csv(death_global_url)

    total_cols = confirmed_global_df.shape[1]
    total_rows = confirmed_global_df.shape[0]
    print(f'Data source Dataframe has {total_rows} rows and {total_cols} cols')

    COL_NAMES = ['Province_State', 'Country_Region', 'Lat', 'Long_'
                                                            'date', 'confirmed', 'deaths']
    date_col = 4  # the index of date column in the df
    insert_list = []
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


config = configparser.ConfigParser()
# config.read('config.cnf')
config.read('../config.cnf')
# str_conn  = 'mysql+pymysql://root:12345678@localhost/bank'
str_conn = 'mysql+pymysql://'
str_conn += config['DATABASE']['user'] + ':' + config['DATABASE']['pw'] + \
            '@' + config['DATABASE']['host'] + '/' + config['DATABASE']['db_name']
print(str_conn)
db = DB(str_conn)

table = db.get_table('covid19_us_raw')
conn = db.get_conn()
logger = db.get_logger()

#extract_us(conn, logger)
extract_global(conn, logger)
