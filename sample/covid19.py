"""
Read Covid-19 csv file from Johns Hopkins
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'


from db.DB import DB
import configparser
import pandas as pd


confirmed_cases_US_url ='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
#confirmed_cases_US_url ='https://api.covidtracking.com/v1/us/daily.csv'
confirmed_case_global_url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
# Save file locally
# localFileName = 'covid-confirmed-US.csv'
# urlretrieve(url, localFileName)

# Read file into a DataFrame and print its head
confirmed_df = pd.read_csv(confirmed_cases_US_url)
print(confirmed_df.shape)
print(confirmed_df.head())

for col in confirmed_df.columns:
    print(col)

print(list(confirmed_df.items())[0])

