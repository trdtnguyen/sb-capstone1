# Correlation between Covid-19 and Economic
## Problem Definition
Covid-19 has attacked humankind for nearly ten months that not only infects millions of people but also affects all aspects of our life. The main reason for those strugglings is we didn’t have enough knowledge about the virus to effectively prevent it. If we know exactly what and when an industry sector is affected by the pandemic, we could have better strategies to address the problem. This project visualized the changes of covid-19 cases and deaths along with related features such as stock prices, unemployment rate, business bankruptcy in the same time frame. For each timeframe, we classify an interesting feature into groups.

In specific, we collect covid-19 data source and other relevant data sources that represent our economy and healthcare system such as stock prices, unemployment rate, health care indexes.  visualizes the changes of covid-19 cases along with other interesting features in a same time series. 

## Building the datasets
In this section, we describle how to get data from datasources.

### Covid-19

* [Data source](https://covidtracking.com/data)
* Get data as: API, csv
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

### Unemployment rate

### Businesses closed/bankruptcy

### Healthcare index
