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
[ERD chart](sql/erd.pdf)
* Raw tables: Tables used to extract raw data. Names include suffix `_raw`.
* Dimentional tables: Tables built from transforming raw tables. Names include suffix `_dim`.
* Fact tables: Tables build from transforming raw tables. Names inculde suffix `_fact`.

Table                 | Rows     | Columns | AVG row length | Table size | Period                     | Description
----------------------|----------|---------|----------------|------------|----------------------------|-------------
`covid19_us_raw`      |927,708   | 15      |139             | 123.7 MiB  |2020.01.22 - 2020.10.25     | Raw table for covid19 in the US
`covid19_global_raw`  | 73,482   | 7       |78              |   5.5 MiB  |2020.01.22 - 2020.10.25     | Raw table for covid19 in the global
`covid19_us_dim`      |  3,221   | 12      |152             | 480 KiB    |2020.01.22 - 2020.10.25     | Dim table for covid19 in the US
`covid19_us_fact`     |  3,221   | 12      |152              | 480 KiB    |2020.01.22 - 2020.10.25     | Dim table for covid19 in the US
`country_dim`         |165       | 16      |397             |  64 KiB    |N/A                         | Dim table for countries

## 4. Building the datasets (Extract)
In this section, we describle in detail how to get data from datasources.

### Covid-19
* This datase has two subsets: global and the US.
* [Data source](https://covidtracking.com/data), [JHU CSSE Covid-19 Dataset](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)
  * [Confirmed cases US](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv)
  * [Deaths US](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv)
  * [Confirmed cases global](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv)
  * [Deaths global](https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv)
* Type: Time series
* Supported methods: API, csv
* Frequency: daily
* Interested features:
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

* US Raw data sample:
```
  UID iso2 iso3  code3  ...  10/15/20 10/16/20 10/17/20 10/18/20
0  84001001   US  USA    840  ...      1949     1966     1983     1989
1  84001003   US  USA    840  ...      6285     6333     6350     6369
2  84001005   US  USA    840  ...       965      968      977      981
3  84001007   US  USA    840  ...       761      771      775      785
4  84001009   US  USA    840  ...      1768     1783     1807     1827
```
* Global Raw data sample:
```
Province/State Country/Region       Lat  ...  10/16/20  10/17/20  10/18/20
0            NaN    Afghanistan  33.93911  ...     40073     40141     40200
1            NaN        Albania  41.15330  ...     16501     16774     17055
2            NaN        Algeria  28.03390  ...     53998     54203     54402
3            NaN        Andorra  42.50630  ...      3377      3377      3377
4            NaN         Angola -11.20270  ...      7222      7462      7622
```

***Important Note*** The raw data is in coloumn-oriented format. Data of a new day is append as new column.

US:
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
Global: 
```
Province/State NaN
Country/Region Albania
Lat 41.1533
Long 20.1683
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
* `stock_ticker_raw` table:

```sql
CREATE TABLE IF NOT EXISTS stock_ticker_raw(
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(128), -- full name of the stock ticker
    industry VARCHAR(64) NULL,
    subindustry VARCHAR(64) NULL,
    hq_location VARCHAR(64) NULL,
    date_first_added datetime NULL,
    cik VARCHAR(10) NULL, -- A Central Index Key or CIK number
    founded_year int NULL,
    PRIMARY KEY(ticker)
);
```

* `stock_price_raw` table:
```sql
CREATE TABLE IF NOT EXISTS stock_price_raw(
    stock_ticker VARCHAR(16) NOT NULL,
    date datetime NOT NULL,
    High double NOT NULL,
    Low double NOT NULL,
    Open double NOT NULL,
    Close double NOT NULL,
    Volume double NOT NULL,
    adj_close double NOT NULL
);
```

* How to extract data
  * First, we get the master list of all stock stickers using the API.
  * Next, for each stock sticker, we get the stock prices data using Yahoo's API. 
  * Extract data into the raw table.

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
* Stock price data sample
```
Date          High        Low       Open      Close       Volume     Adj Close
                                                                          
2020-01-02  75.150002  73.797501  74.059998  75.087502  135480400.0  74.573036
2020-01-03  75.144997  74.125000  74.287498  74.357498  146322800.0  73.848030
2020-01-06  74.989998  73.187500  73.447502  74.949997  118387200.0  74.436470
2020-01-07  75.224998  74.370003  74.959999  74.597504  108872000.0  74.086395
2020-01-08  76.110001  74.290001  74.290001  75.797501  132079200.0  75.278160
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
* How to extract data:
  * First, we build the master list of interested series (e.g., LNS14000000, LNS14000009, LNS14000003, etc) and keep the data in `bol_series_dim`.
```sql
CREATE TABLE IF NOT EXISTS bol_series_dim(
    series_id VARCHAR(64) UNIQUE NOT NULL, -- matched with series_id from bol_raw
    category VARCHAR(256) NOT NULL, -- main category
    subcat1 VARCHAR(256), -- subcategory 1
    subcat2 VARCHAR(256), -- subcategory 1
    PRIMARY KEY(series_id)
);
INSERT INTO BOL_series_dim VALUES('LNS14000000', 'Unemployment Rate', 'overall', '');
INSERT INTO BOL_series_dim VALUES('LNS14000006', 'Unemployment Rate', 'race', 'Black or African American');
INSERT INTO BOL_series_dim VALUES('LNS14000009', 'Unemployment Rate', 'race', 'Hispanic or Latino');
INSERT INTO BOL_series_dim VALUES('LNS14000003', 'Unemployment Rate', 'race', 'White');
INSERT INTO BOL_series_dim VALUES('LNS14000003', 'Unemployment Rate', 'race', 'Asian');
INSERT INTO BOL_series_dim VALUES('LNU04032215', 'Unemployment Rate', 'occupation', 'Management, Professional, and Related Occupations');
INSERT INTO BOL_series_dim VALUES('LNU04032218', 'Unemployment Rate', 'occupation', 'Service');
INSERT INTO BOL_series_dim VALUES('LNU04032219', 'Unemployment Rate', 'occupation', 'Sales and Office Occupations');
INSERT INTO BOL_series_dim VALUES('LNU04032222', 'Unemployment Rate', 'occupation', 'Natural Resources, Construction, and Maintenance Occupations');
INSERT INTO BOL_series_dim VALUES('LNU04032226', 'Unemployment Rate', 'occupation', 'Production, Transportation and Material Moving Occupations');
```
  * Next, for each serie, we extract data from data source using HTTP request.
  * We extract data to raw tables.
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
* Sample data
```
CUUR0000SA0.txt
+-------------+------+--------+---------+-----------+
|  series id  | year | period |  value  | footnotes |
+-------------+------+--------+---------+-----------+
| CUUR0000SA0 | 2020 |  M09   | 260.280 |           |
| CUUR0000SA0 | 2020 |  M08   | 259.918 |           |
| CUUR0000SA0 | 2020 |  M07   | 259.101 |           |
| CUUR0000SA0 | 2020 |  M06   | 257.797 |           |
| CUUR0000SA0 | 2020 |  M05   | 256.394 |           |
| CUUR0000SA0 | 2020 |  M04   | 256.389 |           |
| CUUR0000SA0 | 2020 |  M03   | 258.115 |           |
| CUUR0000SA0 | 2020 |  M02   | 258.678 |           |
| CUUR0000SA0 | 2020 |  M01   | 257.971 |           |
| CUUR0000SA0 | 2019 |  M12   | 256.974 |           |
| CUUR0000SA0 | 2019 |  M11   | 257.208 |           |
| CUUR0000SA0 | 2019 |  M10   | 257.346 |           |
| CUUR0000SA0 | 2019 |  M09   | 256.759 |           |
| CUUR0000SA0 | 2019 |  M08   | 256.558 |           |
| CUUR0000SA0 | 2019 |  M07   | 256.571 |           |
| CUUR0000SA0 | 2019 |  M06   | 256.143 |           |
| CUUR0000SA0 | 2019 |  M05   | 256.092 |           |
| CUUR0000SA0 | 2019 |  M04   | 255.548 |           |
| CUUR0000SA0 | 2019 |  M03   | 254.202 |           |
| CUUR0000SA0 | 2019 |  M02   | 252.776 |           |
| CUUR0000SA0 | 2019 |  M01   | 251.712 |           |
+-------------+------+--------+---------+-----------+

SUUR0000SA0.txt
+-------------+------+--------+---------+-----------+
|  series id  | year | period |  value  | footnotes |
+-------------+------+--------+---------+-----------+
| SUUR0000SA0 | 2020 |  M09   | 146.072 |  Initial  |
| SUUR0000SA0 | 2020 |  M08   | 145.853 |  Initial  |
| SUUR0000SA0 | 2020 |  M07   | 145.405 |  Initial  |
| SUUR0000SA0 | 2020 |  M06   | 144.651 |  Interim  |
| SUUR0000SA0 | 2020 |  M05   | 143.800 |  Interim  |
| SUUR0000SA0 | 2020 |  M04   | 143.847 |  Interim  |
| SUUR0000SA0 | 2020 |  M03   | 145.005 |  Interim  |
| SUUR0000SA0 | 2020 |  M02   | 145.390 |  Interim  |
| SUUR0000SA0 | 2020 |  M01   | 144.995 |  Interim  |
| SUUR0000SA0 | 2019 |  M12   | 144.437 |  Interim  |
| SUUR0000SA0 | 2019 |  M11   | 144.613 |  Interim  |
| SUUR0000SA0 | 2019 |  M10   | 144.722 |  Interim  |
| SUUR0000SA0 | 2019 |  M09   | 144.428 |           |
| SUUR0000SA0 | 2019 |  M08   | 144.388 |           |
| SUUR0000SA0 | 2019 |  M07   | 144.409 |           |
| SUUR0000SA0 | 2019 |  M06   | 144.243 |           |
| SUUR0000SA0 | 2019 |  M05   | 144.183 |           |
| SUUR0000SA0 | 2019 |  M04   | 143.926 |           |
| SUUR0000SA0 | 2019 |  M03   | 143.297 |           |
| SUUR0000SA0 | 2019 |  M02   | 142.571 |           |
| SUUR0000SA0 | 2019 |  M01   | 142.001 |           |
+-------------+------+--------+---------+-----------+
```
### Transportation
* [Air Carrier Statistics (Form 41 Traffic)- U.S. Carriers](https://www.transtats.bts.gov/Tables.asp?DB_ID=110&DB_Name=Air%20Carrier%20Statistics%20%28Form%2041%20Traffic%29-%20%20U.S.%20Carriers&DB_Short_Name=Air%20Carriers)
* [Air Carriers : T-100 Domestic Segment (U.S. Carriers)](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=259) (Latest Available Data: July 2020)
### Environment:
* [National Center for Environmental Information](https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation)
* [AirNow Web service](https://docs.airnowapi.org/webservices): Get daily air quality index
### Businesses closed/bankruptcy

### Healthcare index

## 5. Transform Data
This stage transform data from the raw tables to our fact and demensional tables.

### Covid-19
* `country_dim` table: 
  * Dimentional table keeps general informaiton of countries in the world.
  * Aggregate data from States/Province of a country (if any)
  * We buit this table by joining `covid19_global_raw` and `world.country` (provided in MySQL workbench)
  
```sql
CREATE TABLE IF NOT EXISTS country_dim(
    code VARCHAR(3), -- country's code in 3 leters e.g., CAN
    Name VARCHAR(32), -- country's name e.g., Canada
    Lat double NOT NULL,
    Long_ double NOT NULL,
    Continent VARCHAR(32), -- e.g., North America
    Region VARCHAR(32), -- e.g., North America
    SurfaceArea double,
    IndepYear int,
    Population int,
    LifeExpectancy double,
    GNP int,
    LocalName VARCHAR(32),
    GovernmentForm VARCHAR(32),
    HeadOfState VARCHAR(32),
    Captital int,
    Code2 VARCHAR(3),
    PRIMARY KEY(code)
);
```
* `covid19_global_fact` table:
  * Fact table keep series data of covid19 confirmed cases and deaths.
  * Transformed from raw table `convid19_global_raw`
  * `dateid` is an interger in format of `yyyymmdd` computed from `date`
```sql
CREATE TABLE IF NOT EXISTS covid19_global_fact(
    dateid bigint NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, country_code),
    FOREIGN KEY (country_code) REFERENCES country_dim(code)
);
```

### Stock Prices
* `stock_price_fact` table: 
  * Fact table keep series data for ***daily*** stock prices
  * Remind that we've built `stock_ticker_raw` in the Extract stage. This table use stock_ticker as a foreign key.
  * `dateid` is an interger in format of `yyyymmdd` computed from `date`
  ```sql
  CREATE TABLE IF NOT EXISTS stock_price_fact(
	   dateid BIGINT NOT NULL, -- number in YYYYmmdd format
    stock_ticker VARCHAR(16) NOT NULL,
    date datetime NOT NULL,
    high double NOT NULL,
    low double NOT NULL,
    open double NOT NULL,
    close double NOT NULL,
    volume double NOT NULL,
    adj_close double NOT NULL,
    
    PRIMARY KEY(dateid, stock_ticker),
    FOREIGN KEY(stock_ticker) REFERENCES stock_ticker_raw(ticker)
  );

  ```
### Employment / Unemployment rate
* `bol_series_fact` table:
  * Fact table keep series data for ***monthly*** statistics feature provided by [U.S Bureau of Labor](https://www.bls.gov/data/)
  * Transformed from raw table `bol_raw`
  *  `dateid` is an interger in format of `yyyymmdd` computed from `date`. 
  * we convert `date` from `bol_raw.year` and `bol_raw.period`. Since the data is monthly published, the day value always show as '01'.
```sql
CREATE TABLE IF NOT EXISTS bol_series_fact(
	   dateid BIGINT NOT NULL, -- id = YYYYMM e.g., 202009 is data for Sep of 2020
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    date datetime NOT NULL, -- monthly
    value double,
    footnotes  varchar(128),
    PRIMARY KEY(dateid, series_id),
    
    FOREIGN KEY(series_id) references BOL_series_dim(series_id)
);
```
