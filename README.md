# Correlation between Covid-19 and Economic
## Problem Definition
Covid-19 has attacked humankind for nearly ten months that not only infects millions of people but also affects all aspects of our life. The main reason for those strugglings is we didn’t have enough knowledge about the virus to effectively prevent it. If we know exactly what and when an industry sector is affected by the pandemic, we could have better strategies to address the problem. This project visualized the changes of covid-19 cases and deaths along with related features such as stock prices, unemployment rate, business bankruptcy in the same time frame. For each timeframe, we classify an interesting feature into groups.

In specific, we collect covid-19 data source and other relevant data sources that represent our economy and healthcare system such as stock prices, unemployment rate, health care indexes.  visualizes the changes of covid-19 cases along with other interesting features in a same time series. 

## Building the datasets
In this section, we describle how to get data from datasources.

### Covid-19

* [Data source](https://covidtracking.com/data)
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
  
### Stock Prices
* Using `yahoo-finance` package ([guide](https://pypi.org/project/yahoo-finance/))
* Type: Time series
* Supported methods: API, json
* Frequency: daily
* We focus on popular stock market in the US such as S&P500, Dow Jones, and Nasdaq

```
$ pip install yahoo-finance
```

```
from yahoo_finance import Share
```

### Employment / Unemployment rate
* [Data source](https://www.bls.gov/data/)
* Type: Time series
* Supported methods: API, json
* Frequency: monthly
* Interested features:
  * Unemployment Rate
  * Unemployment Rate - Black or African American
  * Unemployment Rate - Hispanic or Latino
  * Unemployment Rate - White
  * Unemployment Rate - Asian

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
