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


confirmed_cases_US_url ='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
death_US_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'

confirmed_case_global_url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
death_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'

"""
10/18/20 -> 2020-10-18
"""
def convert_date(in_date):
    d_list = in_date.split('/')
    out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]

    return out_date

def extract_us():
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

    # insert_list = [{'UID': 84001001, 'iso2': 'US', 'iso3': 'USA', 'code3': 840, 'FIPS': 1001.0, 'Admin2': 'Autauga',
    #                 'Province_State': 'Alabama', 'Country_Region': 'US', 'Lat': 32.53952745, 'Long_': -86.64408227,
    #                 'Combined_Key': 'Autauga, Alabama, US', 'date': '2020-10-18', 'confirmed': 1989, 'deaths': 0},
    #                {'UID': 84001003, 'iso2': 'US', 'iso3': 'USA', 'code3': 840, 'FIPS': 1003.0, 'Admin2': 'Baldwin',
    #                 'Province_State': 'Alabama', 'Country_Region': 'US', 'Lat': 30.72774991, 'Long_': -87.72207058,
    #                 'Combined_Key': 'Baldwin, Alabama, US', 'date': '2020-10-18', 'confirmed': 6369, 'deaths': 0}]
    # try:
    #
    #     results = conn.execute(table.insert(), insert_list)
    #     print(f'{results.rowcount} rows inserted')
    # except DBAPIError:
    #     logger.error('Error Query')

    confirmed_us_df = pd.read_csv(confirmed_cases_US_url)
    death_us_df = pd.read_csv(death_US_url)
    total_cols = confirmed_us_df.shape[1]
    total_rows = confirmed_us_df.shape[0]

    COL_NAMES = ['UID', 'iso2', 'iso3', 'code3', 'FIPS',
                 'Admin2', 'Province_State', 'Country_Region',
                 'Lat', 'Long_', 'Combined_Key',
                 'date', 'confirmed', 'deaths']
    date_col = 11 # the index of date column

    insert_list = []
    print("Extract data from data sources ...")
    for i in range(total_rows):
        # build the dictionary with key : value is column_name : value
        insert_val = {}
        insert_val['UID'] = int(confirmed_us_df.iloc[i]['UID'])
        insert_val['iso2'] = confirmed_us_df.iloc[i]['iso2']
        insert_val['iso3'] = confirmed_us_df.iloc[i]['iso3']
        insert_val['code3'] = int(confirmed_us_df.iloc[i]['code3'])
        insert_val['FIPS'] = float(confirmed_us_df.iloc[i]['FIPS'])
        insert_val['Admin2'] = confirmed_us_df.iloc[i]['Admin2']
        insert_val['Province_State'] = confirmed_us_df.iloc[i]['Province_State']
        insert_val['Country_Region'] = confirmed_us_df.iloc[i]['Country_Region']
        insert_val['Lat'] = float(confirmed_us_df.iloc[i]['Lat'])
        insert_val['Long_'] = float(confirmed_us_df.iloc[i]['Long_'])
        insert_val['Combined_Key'] = confirmed_us_df.iloc[i]['Combined_Key']


        # for coln in range(date_col):
        #     insert_val[COL_NAMES[coln]] = confirmed_us_df.iloc[i][COL_NAMES[coln]]

        # Get date columns from date_col to the last column
        for j in range(date_col, total_cols):
            date = confirmed_us_df.columns[j]
            date = convert_date(date)
            val = confirmed_us_df.iloc[i][j]
            insert_val['date'] = date
            insert_val['confirmed'] = int(val)
            insert_val['deaths'] = 0

        insert_list.append(insert_val)

    print("Extract data Done.")
    print("Insert to database...")
    # insert database
    try:
        #print(insert_list)
        results = conn.execute(table.insert(), insert_list)
        print(f'{results.rowcount} rows inserted')
        print("Insert to database Done.")
    except DBAPIError:
        logger.error('Error Query')

extract_us()
# d = convert_date('10/18/20')
# print(d)