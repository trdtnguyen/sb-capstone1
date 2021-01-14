DROP TABLE IF EXISTS latest_data;
DROP TABLE IF EXISTS covid19_us_monthly_fact;
DROP TABLE IF EXISTS covid19_us_fact;
DROP TABLE IF EXISTS covid19_us_dim;
DROP TABLE IF EXISTS covid19_global_monthly_fact;
DROP TABLE IF EXISTS covid19_global_fact;
DROP TABLE IF EXISTS country_dim;
DROP TABLE IF EXISTS covid19_global_raw;
DROP TABLE IF EXISTS covid19_us_raw;

DROP TABLE IF EXISTS stock_price_monthly_fact;
DROP TABLE IF EXISTS stock_price_fact;
DROP TABLE IF EXISTS stock_ticker_raw;
DROP TABLE IF EXISTS stock_price_raw;

DROP TABLE IF EXISTS bol_series_fact;
DROP TABLE IF EXISTS bol_series_dim;
DROP TABLE IF EXISTS bol_raw;

/*global table for resuming extraction*/
CREATE TABLE IF NOT EXISTS latest_data(
    table_name VARCHAR(128) NOT NULL,
    latest_date datetime NOT NULL,
    PRIMARY KEY (table_name)
);
INSERT INTO latest_data VALUES("covid19_us_monthly_fact", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_us_fact", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_us_dim", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_global_monthly_fact", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_global_fact", "1990-01-01");
INSERT INTO latest_data VALUES("country_dim", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_global_raw", "1990-01-01");
INSERT INTO latest_data VALUES("covid19_us_raw", "1990-01-01");
INSERT INTO latest_data VALUES("stock_price_monthly_fact", "1990-01-01");
INSERT INTO latest_data VALUES("stock_price_fact", "1990-01-01");
INSERT INTO latest_data VALUES("stock_ticker_raw", "1990-01-01");
INSERT INTO latest_data VALUES("stock_price_raw", "1990-01-01");
INSERT INTO latest_data VALUES("bol_series_fact", "1990-01-01");
INSERT INTO latest_data VALUES("bol_series_dim", "1990-01-01");
INSERT INTO latest_data VALUES("bol_raw", "1990-01-01");

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
    Province_State VARCHAR(32),
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
    Province_State VARCHAR(32), -- State name e.g., Alabama
    Country_Region VARCHAR(32), -- Country name e.g., US
    Lat double NOT NULL,
    Long_ double NOT NULL,
    Combined_Key VARCHAR(128), -- e.g., Autauga, Alabama, US
    Population INT,
    PRIMARY KEY (UID)
);

CREATE TABLE IF NOT EXISTS covid19_us_fact (
	dateid bigint NOT NULL,
    UID bigint NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, UID),
    FOREIGN KEY(UID) REFERENCES covid19_us_dim(UID)
);

CREATE TABLE IF NOT EXISTS covid19_us_monthly_fact (
	dateid bigint NOT NULL,
    UID bigint NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, UID),
    FOREIGN KEY(UID) REFERENCES covid19_us_dim(UID)
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

CREATE TABLE IF NOT EXISTS covid19_global_fact(
    dateid bigint NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, country_code),
    FOREIGN KEY (country_code) REFERENCES country_dim(code)
);

CREATE TABLE IF NOT EXISTS covid19_global_monthly_fact(
    dateid bigint NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    date datetime NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    month_name VARCHAR(32),
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, country_code),
    FOREIGN KEY (country_code) REFERENCES country_dim(code)
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
    FOREIGN KEY(stock_ticker) REFERENCES stock_ticker_raw(ticker)
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
    FOREIGN KEY(stock_ticker) REFERENCES stock_ticker_raw(ticker)
);

CREATE TABLE IF NOT EXISTS bol_raw(
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

/*fact table fetching data from data source*/
CREATE TABLE IF NOT EXISTS bol_series_fact(
	dateid BIGINT NOT NULL, -- id = YYYYMM e.g., 202009 is data for Sep of 2020
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    date datetime NOT NULL, -- monthly
    value double,
    footnotes  varchar(128),
    PRIMARY KEY(dateid, series_id),
    
    FOREIGN KEY(series_id) references bol_series_dim(series_id)
);


