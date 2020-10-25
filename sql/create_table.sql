
DROP TABLE IF EXISTS covid19_global_raw;
DROP TABLE IF EXISTS covid19_us_raw;
DROP TABLE IF EXISTS stock_price_raw;
DROP TABLE IF EXISTS BOL_raw;

DROP TABLE IF EXISTS covid19_us_fact;
DROP TABLE IF EXISTS covid19_us_dim;
DROP TABLE IF EXISTS covid19_global_fact;

DROP TABLE IF EXISTS stock_ticker_dim;
DROP TABLE IF EXISTS stock_price_fact;

DROP TABLE IF EXISTS BOL_series_dim;
DROP TABLE IF EXISTS BOL_series_fact;

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
    FIPS double NOT NULL, -- ???
    Admin2 VARCHAR(32), -- County name e.g., Autauga
    Province_State VARCHAR(32), -- State name e.g., Alabama
    Country_Region VARCHAR(32), -- Country name e.g., US
    Lat double NOT NULL,
    Long_ double NOT NULL,
    Combined_Key VARCHAR(128), -- e.g., Autauga, Alabama, US
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

CREATE TABLE IF NOT EXISTS covid19_global_fact(
    dateid bigint NOT NULL,
    Province_State VARCHAR(32),
    Country_Region VARCHAR(64), -- country 
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, Country_Region)
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
INSERT INTO stock_price_raw VALUES('MMM', '2020-01-01', 10.0, 5.0, 5.0, 10.0, 123, 12);
INSERT INTO stock_price_raw VALUES('MMA', '2020-01-02', 10.0, 5.0, 5.0, 10.0, 123, 12);

/*dimension table stock, represents for each stock ticker*/
CREATE TABLE IF NOT EXISTS stock_ticker_dim(
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(64), -- full name of the stock ticker
    PRIMARY KEY(ticker)
);

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
    FOREIGN KEY(stock_ticker) REFERENCES stock_ticker_dim(ticker)
);

CREATE TABLE IF NOT EXISTS BOL_raw(
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    year INT NOT NULL, 
    period VARCHAR(8), -- month
    value double,
    footnotes  varchar(128)
);
/*dimension table series, translate from series_id to human-reading text
Each row is an desired feature
*/
CREATE TABLE IF NOT EXISTS BOL_series_dim(
    series_id VARCHAR(64) UNIQUE NOT NULL, -- matched with series_id from raw data
    name VARCHAR(64) NOT NULL, -- the meaning of series_id
    PRIMARY KEY(series_id)
);



/*fact table fetching data from data source*/
CREATE TABLE IF NOT EXISTS BOL_series_fact(
	dateid BIGINT NOT NULL, -- id = YYYYMM e.g., 202009 is data for Sep of 2020
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    date datetime NOT NULL, -- monthly
    value double,
    footnotes  varchar(128),
    PRIMARY KEY(dateid, series_id),
    
    FOREIGN KEY(series_id) references BOL_series_dim(series_id)
);


