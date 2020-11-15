"""
Extract Covid-19 data from Johns Hopkins' data source
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from db.DB import DB
import pandas as pd
import configparser
from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from sqlalchemy.sql import text

from datetime import timedelta, datetime
import pymysql
from os import environment as env

import pickle
import requests
import bs4 as bs

DEFAULT_TICKER_FILE = 'sp500tickers.txt'
DEFAULT_ERROR_TICKER_FILE='sp500_error_tickers.txt'

def convert_date(in_date):
    d_list = in_date.split('/')
    out_date = '20' + d_list[2] + '-' + d_list[0] + '-' + d_list[1]

"""
Helper function for iterating from stat_date to end_date
"""
def daterange(start_date, end_date):
    numday = int((end_date - start_date).days)
    for n in range(numday):
        yield start_date + timedelta(n)

class Stock:
    def __init__(self):
        user = env.get('MYSQL_USER')
        db_name = env.get('MYSQL_DATABASE')
        pw = env.get('MYSQL_PASSWORD')
        host = env.get('MYSQL_HOST')

        config = configparser.ConfigParser()
        # config.read('config.cnf')
        config.read('../config.cnf')
        # str_conn  = 'mysql+pymysql://root:12345678@localhost/bank'
        str_conn = f'mysql+pymysql://{user}:{pw}@{host}/{db_name}'
        self.db = DB(str_conn)
        self.conn = self.db.get_conn()
        self.logger = self.db.get_logger()

    """
    Extract list of sp500 tickers and write the list on file
    Return: the list of sp500 tickers
    """
    def extract_sp500_tickers(self):
        conn = self.conn
        logger = self.logger

        TABLE_NAME = 'stock_ticker_raw'
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})
        tickers = []
        insert_list = []
        for row in table.findAll('tr')[1:]:

            insert_val = {}
            ticker = row.findAll('td')[0].text
            ticker = ticker.rstrip('\n')
            tickers.append(ticker)
            insert_val['ticker'] = ticker

            insert_val['name'] = (row.findAll('td')[1].text).rstrip('\n') # Security
            insert_val['industry'] = (row.findAll('td')[3].text).rstrip('\n') # GICS Sector
            insert_val['subindustry'] = (row.findAll('td')[4].text).rstrip('\n')  # GICS Sub-industry
            insert_val['hq_location'] = row.findAll('td')[5].text.rstrip('\n')  # HQ location

            if len(row.findAll('td')[6].text) > 12:
                insert_val['date_first_added'] = row.findAll('td')[6].text.rstrip('\n')[:10]  # date_first_added
            elif len(row.findAll('td')[6].text) == 0:
                insert_val['date_first_added'] = '1000-01-01' # NULL
            else:
                insert_val['date_first_added'] = (row.findAll('td')[6].text).rstrip('\n')  # date_first_added
            insert_val['cik'] = (row.findAll('td')[7].text).rstrip('\n')  # cik
            insert_val['founded_year'] = int((row.findAll('td')[7].text).rstrip('\n'))  # founded_year

            insert_list.append(insert_val)

        with open(DEFAULT_TICKER_FILE,"w") as f:
            f.writelines("%s\n" % ticker for ticker in tickers)
            #pickle.dump(tickers,f)

        df = pd.DataFrame(insert_list)
        try:
            df.to_sql(TABLE_NAME, conn, schema=None, if_exists='append', index=False)
            # manually insert a dictionary will not work due to the limitation number of records insert
            # results = conn.execute(table.insert(), insert_list)
        except ValueError:
            logger.error(f'Error Query when extracting data for {TABLE_NAME} table')
        return tickers


    def extract_single_date_stock(self,
                      reload=True, ticker_file=DEFAULT_TICKER_FILE,
                      date=datetime.now()):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'stock_price_raw'
        # 1. Get list of tickers
        if reload:
            tickers = self.extract_sp500_tickers()
        else:
            # read from file
            with open(ticker_file, "r") as f:
                tickers = f.readlines()
                # tickers = pickle.load(f)

        date_str = date.strftime('%Y-%m-%d')
        COL_NAMES = ['stock_ticker', 'date', 'High', 'Low', 'Open',
                     'Close', 'Volume', 'adj_close']
        print('Extract stocks ...', end=' ')

        total_tickers = len(tickers)
        total_ticker_perc = int(total_tickers / 10)

        # 3. Extract data for all stickers each day. We insert data per day


        noinfo_tickers = []
        insert_list = []
        pct = 0
        cnt = 0
        for ticker in tickers:
            cnt += 1
            if cnt % total_ticker_perc == 0:
                print(f'{int(cnt / total_ticker_perc)}', end=' ')

            try:
                stock_df = data.DataReader(ticker, 'yahoo', date_str, date_str)
                # this data frame has date as index
                for i, row in stock_df.iterrows():
                    insert_val = {}
                    insert_val['stock_ticker'] = ticker
                    insert_val['date'] = i.strftime('%Y-%m-%d')
                    insert_val['High'] = float(row['High'])
                    insert_val['Low'] = float(row['Low'])
                    insert_val['Open'] = float(row['Open'])
                    insert_val['Close'] = float(row['Close'])
                    insert_val['Volume'] = float(row['Volume'])
                    insert_val['adj_close'] = float(row['Adj Close'])

                    insert_list.append(insert_val)
            except:  # catch *all* exceptions
                print(f"No information for ticker {ticker}")
                noinfo_tickers.append(ticker)
                continue

        # write the ticker list that has no info
        if len(noinfo_tickers) > 0:
            print(f"there are {len(noinfo_tickers)} tickers don't have info")
            with open(DEFAULT_ERROR_TICKER_FILE, "w") as f_error:
                f_error.writelines("%s\n" % ticker for ticker in noinfo_tickers)

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

    def extract_batch_stock(self,
                      reload=True, ticker_file=DEFAULT_TICKER_FILE,
                      start_date=datetime.now(), end_date=datetime.now()):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'stock_price_raw'
        # 1. Resuming extract data. We don't need to extract data that we had in the database
        #Get the latest date from database
        s = text("SELECT date "
                 f"FROM {RAW_TABLE_NAME} "
                 "ORDER BY date DESC "
                 "LIMIT 1")
        try:
            ret_list = conn.execute(s).fetchall()
            df_tem = pd.DataFrame(ret_list)
            if len(df_tem) > 0:
                latest_date = df_tem.iloc[0][0]
                if latest_date > end_date:
                    print(f'database last update on {latest_date} '
                          f'that is later than the end date {end_date}. No further extract needed')
                    return
                else:
                    print(f'set start_date to {latest_date}')
                    start_date = latest_date

        except pymysql.OperationalError as e:
            logger.error(f'Error Query when get latest date in {RAW_TABLE_NAME}')
            print(e)

        # 2. Get list of tickers
        if reload:
            tickers = self.extract_sp500_tickers()
        else:
            #read from file
            with open(ticker_file, "r") as f:
                tickers = f.readlines()
                #tickers = pickle.load(f)

        #tickers = ['AAPL', 'MSFT', '^GSPC']
        # start = dt.datetime(2020, 1, 1)
        # end = dt.datetime.now()
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        COL_NAMES = ['stock_ticker', 'date', 'High', 'Low', 'Open',
                     'Close', 'Volume', 'adj_close']

        print('Extract stocks ...', end=' ')

        total_tickers = len(tickers)
        total_ticker_perc = int(total_tickers / 10)

        # 3. Extract data for all stickers


        noinfo_tickers = []

        pct = 0
        cnt = 0
        for ticker in tickers:
            print (f'extract data for {ticker} ...', end=' ')
            insert_list = []
            cnt += 1
            if cnt % total_ticker_perc == 0:
                print(f'{int(cnt/total_ticker_perc)}', end=' ')
            try:
                stock_df = data.DataReader(ticker, 'yahoo', start_date_str, end_date_str)
                # this data frame has date as index
                for i, row in stock_df.iterrows():
                    insert_val = {}
                    insert_val['stock_ticker'] = ticker
                    insert_val['date'] = i.strftime('%Y-%m-%d')
                    insert_val['High'] = float(row['High'])
                    insert_val['Low'] = float(row['Low'])
                    insert_val['Open'] = float(row['Open'])
                    insert_val['Close'] = float(row['Close'])
                    insert_val['Volume'] = float(row['Volume'])
                    insert_val['adj_close'] = float(row['Adj Close'])

                    insert_list.append(insert_val)
            except:  # catch *all* exceptions
                print(f"No information for ticker {ticker}")
                noinfo_tickers.append(ticker)
                continue
            print('Done. Insert into database ...', end=' ')
            df = pd.DataFrame(insert_list)
            try:
                df.to_sql(RAW_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
                # manually insert a dictionary will not work due to the limitation number of records insert
                # results = conn.execute(table.insert(), insert_list)
            except ValueError:
                logger.error(f'Error Query when extracting data for {RAW_TABLE_NAME} table')
            print('Done.')
            #loop the next ticker

        # write the ticker list that has no info
        if len(noinfo_tickers) > 0:
            print(f"there are {len(noinfo_tickers)} tickers don't have info")
            with open(DEFAULT_ERROR_TICKER_FILE, "w") as f_error:
                f_error.writelines("%s\n" % ticker for ticker in noinfo_tickers)

        print('Extract all data Done.')


    """
    Dependencies:
        extract_sp500_tickers ->
        extract_batch_stock() -> 
        transform_raw_to_fact_stock() ->
        aggregate_fact_to_monthly_fact_stock
    """


    def aggregate_fact_to_monthly_fact_stock(self):
        conn = self.conn
        logger = self.logger
        FACT_TABLE_NAME = 'stock_price_fact'
        MONTHLY_FACT_TABLE_NAME = 'stock_price_monthly_fact'

        # 1. Transform from raw to fact table
        print(f'Transform data from {FACT_TABLE_NAME} to {MONTHLY_FACT_TABLE_NAME}.')
        s = text("SELECT stock_ticker, YEAR(date), MONTH(date), MONTHNAME(date), "
                 "AVG(High) , AVG(Low), AVG(Open) , AVG(Close), SUM(Volume) , AVG(adj_close) "
                 f"FROM {FACT_TABLE_NAME} "
                 "GROUP BY stock_ticker, YEAR(date), MONTH(date), MONTHNAME(date) "
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
                insert_val['stock_ticker'] = row[0]
                insert_val['date'] = insert_date
                insert_val['year'] = year
                insert_val['month'] = month
                insert_val['month_name'] = row[3]
                insert_val['High'] = row[4]
                insert_val['Low'] = row[5]
                insert_val['Open'] = row[6]
                insert_val['Close'] = row[7]
                insert_val['Volume'] = row[8]
                insert_val['adj_close'] = row[9]

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
        extract_sp500_tickers ->
        extract_batch_stock() -> 
        transform_raw_to_fact_stock()
    
    """


    def transform_raw_to_fact_stock(self):
        conn = self.conn
        logger = self.logger
        RAW_TABLE_NAME = 'stock_price_raw'
        FACT_TABLE_NAME = 'stock_price_fact'

        # 1. Transform from raw to fact table
        print(f'Transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}.')
        s = text("SELECT stock_ticker, date, High, Low, Open, Close, Volume, adj_close "
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
                insert_val['stock_ticker'] = row[0]
                insert_val['date'] = row[1]
                insert_val['High'] = row[2]
                insert_val['Low'] = row[3]
                insert_val['Open'] = row[4]
                insert_val['Close'] = row[5]
                insert_val['Volume'] = row[6]
                insert_val['adj_close'] = row[7]

                insert_list.append(insert_val)

            df = pd.DataFrame(insert_list)
            if len(df) > 0:
                df.to_sql(FACT_TABLE_NAME, conn, schema=None, if_exists='append', index=False)
        except pymysql.OperationalError as e:
            logger.error(f'Error Query when transform data from {RAW_TABLE_NAME} to {FACT_TABLE_NAME}')
            print(e)
        print('Done.')
#extract_sp500_tickers()

# start_date = datetime(2020, 1, 1)
# end_date = datetime(2020, 10, 23)
# extract_batch_stock(conn, logger, True,DEFAULT_TICKER_FILE, start_date, end_date)