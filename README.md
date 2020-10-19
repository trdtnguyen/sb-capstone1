# Correlation between Covid-19 and Economic
## 1. Problem Definition
Covid-19 has attacked humankind for nearly ten months that not only infects millions of people but also affects all aspects of our life. The main reason for those strugglings is we didn’t have enough knowledge about the virus to effectively prevent it. If we know exactly what and when an industry sector is affected by the pandemic, we could have better strategies to address the problem. This project visualized the changes of covid-19 cases and deaths along with related features such as stock prices, unemployment rate, business bankruptcy in the same time frame. For each timeframe, we classify an interesting feature into groups.

In specific, we collect covid-19 data source and other relevant data sources that represent our economy and healthcare system such as stock prices, unemployment rate, health care indexes.  visualizes the changes of covid-19 cases along with other interesting features in a same time series. 

This project provides the visual view for those questions:
* The effects of the coronavirus (COVID-19) pandemic on the stock market.
  * The most raised and dropped stocks 
  * The most stable/unstable stocks
  * The most promising stocks
* The effects of COVID-19 on the labor market ([source](https://www.bls.gov/cps/effects-of-the-coronavirus-covid-19-pandemic.htm))
  * Top 10 occupations are most effected by Covid-19?
  * What are the unimployment rates by race, occupation during the pendemic?
  * How many working people have to take care of children who cannot go to school?

## 2. Expected Results
This section describe what the final ouputs look like. The visual information will be updated later.

* The main output of this project is the dashboard that displays time serires data of various interested major features (stock market, labor market, economics) along with covid-19 data in the same line chart.
* One major feature could be "drilled" down by multiple sub categories. For example, labor market could be drilled down as employment rate, unimployment rate by races, occupations, gender.
* When two major features have the same measurement unit, they could be displayed in the same dashboard. For example, unimployment rate and employment rate.

## 3. Data model
### Covid-19


## 4. Building the datasets
In this section, we describle in detail how to get data from datasources.

### Covid-19

* [Data source](https://covidtracking.com/data), [JHU CSSE Covid-19 Dataset](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)
  * [Confirmed cases US](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv)
  * [Deaths US](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv)
  * [Confirmed cases global](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv)
  * [Deaths global](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv)
* Type: Time series
* Supported methods: API, csv
* Frequency: daily
* Interested features:
* For each country/area/territory we interested in:
  * Population
  * World bank group (High Income,Upper Middle Income,  Lower Middle Income, Low Income)
  * Total cases
  * Total deaths
  * Total tests
  * Test per case
  
* Sample code:
```python
import pandas as pd
confirmed_case_global_url='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
confirmed_df = pd.read_csv(confirmed_case_global_url)
print(confirmed_df.shape)
print(confirmed_df.head())
```
Raw data sample:
```
  UID iso2 iso3  code3  ...  10/15/20 10/16/20 10/17/20 10/18/20
0  84001001   US  USA    840  ...      1949     1966     1983     1989
1  84001003   US  USA    840  ...      6285     6333     6350     6369
2  84001005   US  USA    840  ...       965      968      977      981
3  84001007   US  USA    840  ...       761      771      775      785
4  84001009   US  USA    840  ...      1768     1783     1807     1827
```

*** Important Note *** The raw data is in coloumn-oriented format. Data of a new day is append as new column.
```
UID 84001001
iso2 US
iso3 USA
code3 840
FIPS 1001.0
Admin2 Autauga
Province_State Alabama
Country_Region US
Lat 32.53952745
Long_ -86.64408227
Combined_Key Autauga, Alabama, US
day1
day2
...
currentday
```

### Stock Prices
* Currently, there are three methods to get stock prices: [Yahoo Finance API](https://pypi.org/project/yahoo-finance/), [Google Finance API](https://pypi.org/project/googlefinance/), and [pandas_datareader](https://learndatasci.com/tutorials/python-finance-part-yahoo-finance-api-pandas-matplotlib/). Only the last one work.

* Type: Time series
* Supported methods: API, json
* Frequency: daily
* We focus on popular stock market in the US such as S&P500, Dow Jones, and Nasdaq


```
pip install pandas-datareader
```

```
from pandas_datareader import data

tickers = ['AAPL', 'MSFT', '^GSPC']
start_date = '2020-01-01'
end_date = '2020-09-30'

for ticker in tickers:
    panel_data = data.DataReader(ticker, 'yahoo', start_date, end_date)
    print(type(panel_data))
    print("ticker: ", ticker)
    print(panel_data)
```

### Employment / Unemployment rate
* Data source: [U.S Bureau of Labor](https://www.bls.gov/data/)
* Type: Time series
* Supported methods: API, json
* Frequency: monthly
* All data from Bureau of Labor has unique series id for each feature. We list out desired features and their corresponding series_id as below
* Interested features:
  * Unemployment Rate (overall): `LNS14000000`
  * Unemployment Rate Races:
    * Black or African American: `LNS14000006`
    * Hispanic or Latino: `LNS14000009`
    * White: `LNS14000003`
    * Asian: `LNS14032183`
    
  * Unemployment Rate - Occupations [source](https://www.bls.gov/webapps/legacy/cpsatab13.htm)
    * Management, Professional, and Related Occupations: `LNU04032215`
    * Service Occupations: `LNU04032218`
    * Sales and Office Occupations: `LNU04032219`
    * Natural Resources, Construction, and Maintenance Occupations: `LNU04032222`
    * Production, Transportation and Material Moving Occupations: `LNU04032226`

```python
import requests
import json
import prettytable
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0','SUUR0000SA0'],"startyear":"2011", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)
for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
       'if 'M01' <= period <= 'M12':'
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()
```

### Businesses closed/bankruptcy

### Healthcare index
