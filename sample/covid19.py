"""
Read Covid-19 csv file from Johns Hopkins
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'


from db.DB import DB
import configparser
import pandas as pd


confirmed_cases_US_url ='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
confirmed_case_global_url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
death_US_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
# Save file locally
# localFileName = 'covid-confirmed-US.csv'
# urlretrieve(url, localFileName)

# Read file into a DataFrame and print its head
confirmed_global_df = pd.read_csv(confirmed_case_global_url)
#confirmed_us_df = pd.read_csv(confirmed_cases_US_url)
confirmed_us_df = pd.read_csv(death_US_url)

# US
print(confirmed_us_df.shape)
print(confirmed_us_df.head())

# for col in confirmed_us_df.columns:
#      print(col)
# i = 0
# for i in range (20):
#     print(confirmed_us_df.iloc[1][i])
#get start day
print("All date columns: ")
total_cols = confirmed_us_df.shape[1]
total_rows = confirmed_us_df.shape[0]
print(f'rows: {total_rows}, columns: {total_cols}' )

for i in range(2):
    str1 = confirmed_us_df.iloc[i]['UID']
    str2 = confirmed_us_df.iloc[i]['Combined_Key']
    print(f'{str1} , {str2}')
    for j in range (11, total_cols):
        date = confirmed_us_df.columns[j]
        val = confirmed_us_df.iloc[i][j]
        print(f'{date} : {val}')


# Global
print(confirmed_global_df.shape)
print(confirmed_global_df.head())

# for col in confirmed_global_df.columns:
#      print(col)
i = 0
for i in range (20):
    print(confirmed_global_df.iloc[1][i])
