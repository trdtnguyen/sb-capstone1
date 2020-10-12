"""
Main
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'


from db.DB import DB
import configparser
from urllib.request import urlretrieve
import pandas as pd
import requests
from bs4 import BeautifulSoup

from yahoo_finance import Share

class Main:
    def __init__(self):
        self._connect_db()

    def _connect_db(self):
        config = configparser.ConfigParser()
        #config.read('config.cnf')
        config.read('../config.cnf')
        # str_conn  = 'mysql+pymysql://root:12345678@localhost/covid19cor'
        str_conn = 'mysql+pymysql://'
        str_conn += config['DATABASE']['user'] + ':' + config['DATABASE']['pw'] + \
                    '@' + config['DATABASE']['host'] + '/' + config['DATABASE']['db_name']
        print(str_conn)
        self.my_db = DB(str_conn)
        # tb = self.my_db.get_table('customers')
        # print(type(tb))
        # print(tb.columns.keys())

    def get_db(self):
        return self.my_db


# main = Main()
# db = main.get_db()
data_loc = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/'
           'csse_covid_19_data/csse_covid_19_time_series'
           '/time_series_covid19_confirmed_global.csv')
confirmed_cases_US_url ='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
#confirmed_cases_US_url ='https://api.covidtracking.com/v1/us/daily.csv'
confirmed_case_global_url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
# Save file locally
# localFileName = 'covid-confirmed-US.csv'
# urlretrieve(url, localFileName)

# Read file into a DataFrame and print its head
confirmed_df = pd.read_csv(confirmed_case_global_url)
print(confirmed_df.shape)
print(confirmed_df.head())

for col in confirmed_df.columns:
    print(col)
