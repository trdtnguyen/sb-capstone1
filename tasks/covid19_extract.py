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

    # print('Confirmed cases in US info:')
    # print(confirmed_us_df.shape)
    # print(confirmed_us_df.head(10))
    # for col in confirmed_us_df.columns:
    #     print(col)
    #
    # print ('Death cases in US info:')
    # print(death_us_df.shape)
    # print(death_us_df.head(10))
    # for col in death_us_df.columns:
    #     print (col)

    COL_NAMES = ['UID', 'iso2', 'iso3', 'code3', 'FIPS',
                 'Admin2', 'Province_State', 'Country_Region',
                 'Lat', 'Long_', 'Combined_Key', 'Population',
                 'date', 'confirmed', 'deaths']
    date_col = 12 # the index of date column

    insert_list = []
    print("Extract data from data sources ...")
    #for i in range(total_rows):
    #for i, row in confirmed_us_df.iterrows():
    print('Extract confirms values from confirmed_us_df ...', end=' ')
    confirm_2d = []
    for i, row in confirmed_us_df.iterrows():
        confirm_2d.append([row[j] for j in range(date_col-1, confirmed_us_df.shape[1])])
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


        # for coln in range(date_col):
        #     insert_val[COL_NAMES[coln]] = confirmed_us_df.iloc[i][COL_NAMES[coln]]

        # Get date columns from date_col to the last column
        for j in range(date_col, death_us_df.shape[1]):
            date = death_us_df.columns[j]
            date = convert_date(date)
            death_num = row[j]
            #confirm_num = confirmed_us_df.iloc[i][j-1] # this is slow
            confirm_num = confirm_2d[i][j-date_col]
            insert_val['date'] = date
            insert_val['confirmed'] = int(confirm_num)
            insert_val['deaths'] = int(death_num)

        insert_list.append(insert_val)

    df = pd.DataFrame(insert_list)

    print(df)
    print("Extract data Done.")
    print("Insert to database...", end= ' ')
    # insert database
    # to_sql(name, con, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)[source]
    try:
        df.to_sql('covid19_us_raw', conn, schema=None, if_exists='append', index=False)
        # manually insert a dictionary will not work due to the limitation number of records insert
        #results = conn.execute(table.insert(), insert_list)
    except ValueError:
        logger.error('Error Query')
    print('Done.')

extract_us()