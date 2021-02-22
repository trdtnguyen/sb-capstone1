DROP TABLE IF EXISTS covid_stock_fact;
DROP TABLE IF EXISTS covid_stock_monthly_fact;
DROP TABLE IF EXISTS covid_stock_bol_monthly_fact;

DROP TABLE IF EXISTS latest_data;
DROP TABLE IF EXISTS covid19_us_monthly_fact;
DROP TABLE IF EXISTS covid19_us_fact;
DROP TABLE IF EXISTS covid19_us_dim;
DROP TABLE IF EXISTS covid19_global_monthly_fact;
DROP TABLE IF EXISTS covid19_global_fact;
DROP TABLE IF EXISTS country_dim;
DROP TABLE IF EXISTS covid19_global_dim;
DROP TABLE IF EXISTS covid19_global_raw;
DROP TABLE IF EXISTS covid19_us_raw;
DROP TABLE IF EXISTS covid19_sum_fact;
DROP TABLE IF EXISTS covid19_sum_monthly_fact;


DROP TABLE IF EXISTS stock_index_monthly_fact;
DROP TABLE IF EXISTS stock_index_fact;
DROP TABLE IF EXISTS stock_price_monthly_fact;
DROP TABLE IF EXISTS stock_price_fact;
DROP TABLE IF EXISTS stock_ticker_raw;
DROP TABLE IF EXISTS stock_price_raw;

DROP TABLE IF EXISTS bol_series_fact;
DROP TABLE IF EXISTS bol_series_dim;
DROP TABLE IF EXISTS bol_series_raw;


/*global table for resuming extraction*/
CREATE TABLE IF NOT EXISTS latest_data(
    table_name VARCHAR(128) NOT NULL,
    latest_date datetime NOT NULL,
    PRIMARY KEY (table_name)
);
SET @default_date = "1990-01-01";
INSERT INTO latest_data VALUES("covid19_us_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("covid19_us_fact", @default_date);
INSERT INTO latest_data VALUES("covid19_us_dim", @default_date);
INSERT INTO latest_data VALUES("covid19_global_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("covid19_global_fact", @default_date);
INSERT INTO latest_data VALUES("country_dim", @default_date);
INSERT INTO latest_data VALUES("covid19_global_raw", @default_date);
INSERT INTO latest_data VALUES("covid19_global_dim", @default_date);
INSERT INTO latest_data VALUES("covid19_us_raw", @default_date);
INSERT INTO latest_data VALUES("covid19_sum_fact", @default_date);
INSERT INTO latest_data VALUES("covid19_sum_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("stock_index_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("stock_index_fact", @default_date);
INSERT INTO latest_data VALUES("stock_price_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("stock_price_fact", @default_date);
INSERT INTO latest_data VALUES("stock_ticker_raw", @default_date);
INSERT INTO latest_data VALUES("stock_price_raw", @default_date);
INSERT INTO latest_data VALUES("bol_series_fact", @default_date);
INSERT INTO latest_data VALUES("bol_series_dim", @default_date);
INSERT INTO latest_data VALUES("bol_series_raw", @default_date);

INSERT INTO latest_data VALUES("covid_stock_fact", @default_date);
INSERT INTO latest_data VALUES("covid_stock_monthly_fact", @default_date);
INSERT INTO latest_data VALUES("covid_stock_bol_monthly_fact", @default_date);

/*dimension table used for extracting raw data*/
CREATE TABLE IF NOT EXISTS covid19_us_raw (
    UID bigint NOT NULL,
    iso2 VARCHAR(16) NOT NULL, -- country code 2 letters e.g., US
    iso3 VARCHAR (16) NOT NULL, -- country code 3 letters e.g., USA
    code3 INT NOT NULL, -- area code e.g., 840
    FIPS double , -- ???
    Admin2 VARCHAR(128), -- County name e.g., Autauga
    Province_State VARCHAR(32), -- State name e.g., Alabama
    Country_Region VARCHAR(32), -- Country name e.g., US
    Lat double NOT NULL,
    Long_ double NOT NULL,
    Combined_Key VARCHAR(128), -- e.g., Autauga, Alabama, US
    Population INT,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL
);


/*
Dimension table for covid-19 info of countries in the global
*/
CREATE TABLE IF NOT EXISTS covid19_global_raw(
    Country_Region VARCHAR(64), -- country 
    Lat double NOT NULL,
    Long_ double NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL
);

CREATE TABLE IF NOT EXISTS covid19_us_dim(
	UID bigint NOT NULL,
    iso2 VARCHAR(16) NOT NULL, -- country code 2 letters e.g., US
    iso3 VARCHAR (16) NOT NULL, -- country code 3 letters e.g., USA
    code3 INT NOT NULL, -- area code e.g., 840
    FIPS double , -- ???
    Admin2 VARCHAR(128), -- County name e.g., Autauga
    Province_State VARCHAR(32) NOT NULL UNIQUE, -- State name e.g., Alabama
    Country_Region VARCHAR(32), -- Country name e.g., US
    Lat double NOT NULL,
    Long_ double NOT NULL,
    Combined_Key VARCHAR(128), -- e.g., Autauga, Alabama, US
    Population INT,
    PRIMARY KEY (UID)
);
/*fact table for the US - State level*/
CREATE TABLE IF NOT EXISTS covid19_us_fact (
	dateid bigint NOT NULL,
    Province_State VARCHAR(32), -- State name e.g., Alabama
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    confirmed_inc int,
    deaths_inc int,
    confirmed_inc_pct DECIMAL(18, 5),
    deaths_inc_pct DECIMAL(18, 5),
    
    PRIMARY KEY(dateid, Province_State),
    FOREIGN KEY (Province_State) REFERENCES covid19_us_dim(Province_State)
);

CREATE TABLE IF NOT EXISTS covid19_us_monthly_fact (
	dateid bigint NOT NULL,
    Province_State VARCHAR(32), -- State name e.g., Alabama
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    confirmed int NOT NULL,
    deaths int NOT NULL,
    confirmed_inc int,
    deaths_inc int,
    confirmed_inc_pct DECIMAL(18, 5),
    deaths_inc_pct DECIMAL(18, 5),

    PRIMARY KEY(dateid, Province_State),
    FOREIGN KEY (Province_State) REFERENCES covid19_us_dim(Province_State)
);


CREATE TABLE IF NOT EXISTS country_dim(
    code VARCHAR(3), -- country's code in 3 leters e.g., CAN
    Name VARCHAR(64), -- country's name e.g., Canada
    Continent VARCHAR(32), -- e.g., North America
    Region VARCHAR(32), -- e.g., North America
    SurfaceArea double,
    IndepYear int,
    Population int,
    LifeExpectancy double,
    GNP int,
    LocalName VARCHAR(64),
    GovernmentForm VARCHAR(64),
    HeadOfState VARCHAR(64),
    Capital int,
    Code2 VARCHAR(3),
    PRIMARY KEY(code)
);

CREATE TABLE IF NOT EXISTS covid19_global_dim(
    Country_Region VARCHAR(64), -- country
    Lat double NOT NULL,
    Long_ double NOT NULL,
    PRIMARY KEY(Country_Region)
);

CREATE TABLE IF NOT EXISTS covid19_global_fact(
    dateid bigint NOT NULL,
    Country_Region VARCHAR(64) , -- country
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    confirmed_inc int,
    deaths_inc int,
    confirmed_inc_pct DECIMAL(18, 5),
    deaths_inc_pct DECIMAL(18, 5),
    
    PRIMARY KEY(dateid, Country_Region),
    FOREIGN KEY(Country_Region) REFERENCES covid19_global_dim(Country_Region)
);

CREATE TABLE IF NOT EXISTS covid19_global_monthly_fact(
    dateid bigint NOT NULL,
    Country_Region VARCHAR(64) , -- country
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    confirmed int NOT NULL,
    deaths int NOT NULL,
    confirmed_inc int,
    deaths_inc int,
    confirmed_inc_pct DECIMAL(18, 5),
    deaths_inc_pct DECIMAL(18, 5),
    
    PRIMARY KEY(dateid, Country_Region),
    FOREIGN KEY(Country_Region) REFERENCES covid19_global_dim(Country_Region)
);

CREATE TABLE IF NOT EXISTS covid19_sum_fact (
	dateid bigint NOT NULL,
    date datetime NOT NULL,

    us_confirmed int NOT NULL,
    us_deaths int NOT NULL,
    us_confirmed_inc int NOT NULL,
    us_deaths_inc int NOT NULL,
    us_confirmed_inc_pct DECIMAL(18, 5),
    us_deaths_inc_pct DECIMAL(18, 5),
    global_confirmed int NOT NULL,
    global_deaths int NOT NULL,
    global_confirmed_inc int,
    global_deaths_inc int,
    global_confirmed_inc_pct DECIMAL(18, 5),
    global_deaths_inc_pct DECIMAL(18, 5),

    PRIMARY KEY(dateid)
);
CREATE TABLE IF NOT EXISTS covid19_sum_monthly_fact (
	dateid bigint NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    us_confirmed int NOT NULL,
    us_deaths int NOT NULL,
    us_confirmed_inc int NOT NULL,
    us_deaths_inc int NOT NULL,
    us_confirmed_inc_pct DECIMAL(18, 5),
    us_deaths_inc_pct DECIMAL(18, 5),
    global_confirmed int NOT NULL,
    global_deaths int NOT NULL,
    global_confirmed_inc int NOT NULL,
    global_deaths_inc int NOT NULL,
    global_confirmed_inc_pct DECIMAL(18, 5),
    global_deaths_inc_pct DECIMAL(18, 5),

    PRIMARY KEY(dateid)
);


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


/*raw table get stickers from datasource*/
CREATE TABLE IF NOT EXISTS stock_ticker_raw(
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(128), -- full name of the stock ticker
    industry VARCHAR(64) NULL,
    subindustry VARCHAR(64) NULL,
    hq_location VARCHAR(64) NULL,
    cik VARCHAR(10) NULL, -- A Central Index Key or CIK number
    founded_year int NULL,
    PRIMARY KEY(ticker)
);

CREATE TABLE IF NOT EXISTS stock_index_fact(
    dateid BIGINT NOT NULL, -- number in YYYYmmdd format
    date datetime NOT NULL,
    dowjones_score double NOT NULL,
    nasdaq100_score double NOT NULL,
    sp500_score double NOT NULL,

    PRIMARY KEY(dateid)
);

CREATE TABLE IF NOT EXISTS stock_price_fact(
	dateid BIGINT NOT NULL, -- number in YYYYmmdd format
    stock_ticker VARCHAR(16) NOT NULL,
    date datetime NOT NULL,
    High double NOT NULL,
    Low double NOT NULL,
    Open double NOT NULL,
    Close double NOT NULL,
    Volume double NOT NULL,
    adj_close double NOT NULL,
    
    PRIMARY KEY(dateid, stock_ticker),
    FOREIGN KEY (stock_ticker) REFERENCES stock_ticker_raw(ticker)
);

CREATE TABLE IF NOT EXISTS stock_index_monthly_fact(
    dateid BIGINT NOT NULL, -- number in YYYYmmdd format
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    dowjones_score double NOT NULL,
    nasdaq100_score double NOT NULL,
    sp500_score double NOT NULL,

    PRIMARY KEY(dateid)
);

CREATE TABLE IF NOT EXISTS stock_price_monthly_fact(
	dateid BIGINT NOT NULL, -- number in YYYYmmdd format
    stock_ticker VARCHAR(16) NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    High double NOT NULL,
    Low double NOT NULL,
    Open double NOT NULL,
    Close double NOT NULL,
    Volume double NOT NULL,
    adj_close double NOT NULL,

    PRIMARY KEY(dateid, stock_ticker),
    FOREIGN KEY (stock_ticker) REFERENCES stock_ticker_raw(ticker)
);

CREATE TABLE IF NOT EXISTS bol_series_raw(
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    year INT NOT NULL, 
    period VARCHAR(8), -- month
    value double,
    footnotes  varchar(128)
);

/*dimension table series, translate from series_id to human-reading text
Each row is an desired feature
*/
CREATE TABLE IF NOT EXISTS bol_series_dim(
    series_id VARCHAR(64) UNIQUE NOT NULL, -- matched with series_id from bol_raw
    category VARCHAR(256) NOT NULL, -- main category
    subcat1 VARCHAR(256), -- subcategory 1
    subcat2 VARCHAR(256), -- subcategory 1
    PRIMARY KEY(series_id)
);
INSERT INTO bol_series_dim VALUES('payems', 'Labor market', 'All Employees, Total Nonfarm', '');
INSERT INTO bol_series_dim VALUES('LNS14000006', 'Labor market', 'Unemployment Rate', 'Black or African American');
INSERT INTO bol_series_dim VALUES('LNS14000009', 'Labor market', 'Unemployment Rate', 'Hispanic or Latino');
INSERT INTO bol_series_dim VALUES('LNS14000003', 'Labor market', 'Unemployment Rate', 'White');
INSERT INTO bol_series_dim VALUES('LNS14032183', 'Labor market', 'Unemployment Rate', 'Asian');
INSERT INTO bol_series_dim VALUES('LNU04032215', 'Labor market', 'Unemployment Rate', 'Management, Professional, and Related Occupations');
INSERT INTO bol_series_dim VALUES('LNU04032218', 'Labor market', 'Unemployment Rate', 'Service');
INSERT INTO bol_series_dim VALUES('LNU04032219', 'Labor market', 'Unemployment Rate', 'Sales and Office Occupations');
INSERT INTO bol_series_dim VALUES('LNU04032222', 'Labor market', 'Unemployment Rate', 'Natural Resources, Construction, and Maintenance Occupations');
INSERT INTO bol_series_dim VALUES('LNU04032226', 'Labor market', 'Unemployment Rate', 'Production, Transportation and Material Moving Occupations');
INSERT INTO bol_series_dim VALUES('CES4348100001', 'Labor market', 'All Employees', 'Air Transportation');
INSERT INTO bol_series_dim VALUES('CES6562000101', 'Labor market', 'All Employees', 'Health Care');
INSERT INTO bol_series_dim VALUES('CES6561000001', 'Labor market', 'All Employees', 'Educational Services');
INSERT INTO bol_series_dim VALUES('CES7071000001', 'Labor market', 'All Employees', 'Arts, Entertainment, and Recreation');
INSERT INTO bol_series_dim VALUES('unrate', 'Labor market', 'Unemployment Rate', 'Overall');
INSERT INTO bol_series_dim VALUES('jtsjol', 'Labor market', 'Job Openings', 'Total Nonfarm');
INSERT INTO bol_series_dim VALUES('LNS13023653', 'Labor market', 'Unemployment Level', 'Job Losers on Layoff');
INSERT INTO bol_series_dim VALUES('VMTD11', 'Production & Business Activity', 'Vehicle Miles Traveled', 'Vehicle Miles Traveled');
INSERT INTO bol_series_dim VALUES('AIRRPMTSID11', 'Production & Business Activity', 'Air Revenue Passenger Miles', 'Air Revenue Passenger Miles');
INSERT INTO bol_series_dim VALUES('MRTSSM7225USN', 'Production & Business Activity', 'Retail Sales', 'Restaurants and Other Eating Places');
INSERT INTO bol_series_dim VALUES('MRTSSM4541USS', 'Production & Business Activity', 'Retail Sales', 'Electronic Shopping and Mail-order Houses');
INSERT INTO bol_series_dim VALUES('MRTSSM4451USS', 'Production & Business Activity', 'Retail Sales', 'Grocery Stores');
INSERT INTO bol_series_dim VALUES('MRTSSM446USS', 'Production & Business Activity', 'Retail Sales', 'Health and Personal Care Stores');

/*fact table fetching data from data source*/
CREATE TABLE IF NOT EXISTS bol_series_fact(
	dateid BIGINT NOT NULL, -- id = YYYYMM e.g., 202009 is data for Sep of 2020
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    date datetime NOT NULL, -- monthly
    value double,
    PRIMARY KEY(dateid, series_id),
    FOREIGN KEY(series_id) REFERENCES bol_series_dim(series_id)
);


/*Join tables aggregated from covid19_us_fact, covid19_global_fact, stock_price_fact*/
CREATE TABLE IF NOT EXISTS covid_stock_fact (
    dateid BIGINT NOT NULL,
    date datetime NOT NULL,
    us_confirmed int NOT NULL,
    us_deaths int NOT NULL,
    us_confirmed_inc int NOT NULL,
    us_deaths_inc int NOT NULL,
    us_confirmed_inc_pct DECIMAL(18, 5),
    us_deaths_inc_pct DECIMAL(18, 5),
    global_confirmed int NOT NULL,
    global_deaths int NOT NULL,
    global_confirmed_inc int NOT NULL,
    global_deaths_inc int NOT NULL,
    global_confirmed_inc_pct DECIMAL(18, 5),
    global_deaths_inc_pct DECIMAL(18, 5),
    sp500_score double NOT NULL,
    nasdaq100_score double NOT NULL,
    dowjones_score double NOT NULL,
    PRIMARY KEY(dateid)
);
CREATE TABLE IF NOT EXISTS covid_stock_monthly_fact (
    dateid BIGINT NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    us_confirmed int NOT NULL,
    us_deaths int NOT NULL,
    us_confirmed_inc int NOT NULL,
    us_deaths_inc int NOT NULL,
    us_confirmed_inc_pct DECIMAL(18, 5),
    us_deaths_inc_pct DECIMAL(18, 5),
    global_confirmed int NOT NULL,
    global_deaths int NOT NULL,
    global_confirmed_inc int NOT NULL,
    global_deaths_inc int NOT NULL,
    global_confirmed_inc_pct DECIMAL(18, 5),
    global_deaths_inc_pct DECIMAL(18, 5),
    sp500_score double NOT NULL,
    nasdaq100_score double NOT NULL,
    dowjones_score double NOT NULL,
    PRIMARY KEY(dateid)
);

CREATE TABLE IF NOT EXISTS covid_stock_bol_monthly_fact (
    dateid BIGINT NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    bol_series_id VARCHAR(64) NOT NULL,
    bol_series_value double NOT NULL,
    us_confirmed int,
    us_deaths int,
    us_confirmed_inc int,
    us_deaths_inc int,
    us_confirmed_inc_pct DECIMAL(18, 5),
    us_deaths_inc_pct DECIMAL(18, 5),
    global_confirmed int,
    global_deaths int,
    global_confirmed_inc int,
    global_deaths_inc int,
    global_confirmed_inc_pct DECIMAL(18, 5),
    global_deaths_inc_pct DECIMAL(18, 5),
    sp500_score double,
    nasdaq100_score double,
    dowjones_score double,
    PRIMARY KEY(dateid, bol_series_id),
    FOREIGN KEY(bol_series_id) REFERENCES bol_series_dim(series_id)
);

