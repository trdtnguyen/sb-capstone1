
DROP TABLE IF EXISTS covid19_country;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS stock_price;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS BOL_series_fact;
DROP TABLE IF EXISTS BOL_series;

/*dimension table country, represents for each country instance*/
CREATE TABLE IF NOT EXISTS country (
	id BIGINT NOT NULL auto_increment,
    name varchar(128) NOT NULL, -- Country/Region
    state varchar(128), -- State/Provice
    latitute double NOT NULL,
    longtitute double NOT NULL,
    
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS covid19_country(
	id BIGINT NOT NULL auto_increment,
    country_id BIGINT NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY (country_id) REFERENCES country(id)
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

CREATE TABLE IF NOT EXISTS covid19_country_BOL_series_fact(
	id BIGINT NOT NULL auto_increment,
    covid19_country_id BIGINT NOT NULL,
    BOL_series_fact_id BIGINT NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY (covid19_country_id) REFERENCES covid19_country(id),
    FOREIGN KEY (BOL_series_fact_id) REFERENCES BOL_series_fact(id)
);

CREATE TABLE IF NOT EXISTS covid19_country_stock_fact(
	id BIGINT NOT NULL auto_increment,
    covid19_country_id BIGINT NOT NULL,
    stock_price_id BIGINT NOT NULL,
    
    PRIMARY KEY(id),
    FOREIGN KEY (covid19_country_id) REFERENCES covid19_country(id),
    FOREIGN KEY (stock_price_id) REFERENCES stock_price(id)
);