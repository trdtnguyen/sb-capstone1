
DROP TABLE IF EXISTS covid19_raw_global;
DROP TABLE IF EXISTS covid19_raw_us;
DROP TABLE IF EXISTS stock_price;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS BOL_series_fact;
DROP TABLE IF EXISTS BOL_series;

/*dimension table used for extracting raw data*/
CREATE TABLE IF NOT EXISTS covid19_raw_us (
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
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(UID)
);

/*
Dimension table for covid-19 info of countries in the global
*/
CREATE TABLE IF NOT EXISTS covid19_raw_global(
	id BIGINT NOT NULL auto_increment,
    Province_State VARCHAR(32),
    Country_Region VARCHAR(64), -- country 
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(id)
);


/*dimension table stock, represents for each stock ticker*/
CREATE TABLE IF NOT EXISTS stock(
	id BIGINT NOT NULL auto_increment,
    ticker VARCHAR(16) UNIQUE NOT NULL,
    name VARCHAR(64), -- full name of the stock ticker
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS stock_price(
	id BIGINT NOT NULL auto_increment,
    stock_ticker VARCHAR(16) NOT NULL,
    date datetime NOT NULL,
    high double NOT NULL,
    low double NOT NULL,
    open double NOT NULL,
    close double NOT NULL,
    volume double NOT NULL,
    adj_close double NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY(stock_ticker) REFERENCES stock(ticker)
);

/*
Data retrieve from U.S Bureau of Labor Statistics follow below format:
series_id | year | period | value | footnotes
*/

/*dimension table series, translate from series_id to human-reading text
Each row is an desired feature
*/
CREATE TABLE IF NOT EXISTS BOL_series(
	id BIGINT NOT NULL auto_increment,
    series_id VARCHAR(64) UNIQUE NOT NULL, -- matched with series_id from raw data
    name VARCHAR(64) NOT NULL, -- the meaning of series_id
    PRIMARY KEY(id)
);

/*fact table fetching data from data source*/
CREATE TABLE IF NOT EXISTS BOL_series_fact(
	id BIGINT NOT NULL auto_increment,
    series_id VARCHAR(64) NOT NULL, -- matched with series_id from raw data
    date datetime NOT NULL,
    value double,
    note varchar(128),
    PRIMARY KEY(id),
    
    FOREIGN KEY(series_id) references BOL_series(series_id)
);

CREATE TABLE IF NOT EXISTS covid19_us_BOL_series_fact(
	id BIGINT NOT NULL auto_increment,
    covid19_us_id BIGINT NOT NULL,
    BOL_series_fact_id BIGINT NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY (covid19_us_id) REFERENCES covid19_us(id),
    FOREIGN KEY (BOL_series_fact_id) REFERENCES BOL_series_fact(id)
);

CREATE TABLE IF NOT EXISTS covid19_global_stock_fact(
	id BIGINT NOT NULL auto_increment,
    covid19_country_id BIGINT NOT NULL,
    stock_price_id BIGINT NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY (covid19_country_id) REFERENCES covid19_country(id),
    FOREIGN KEY (stock_price_id) REFERENCES stock_price(id)
);