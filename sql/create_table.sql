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
SELECT Country_Region, date, SUM(confirmed), SUM(deaths)
FROM covid19_global_raw
GROUP BY Country_Region, date
ORDER BY Country_Region
;

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
SELECT UID, YEAR(date), MONTH(date), MONTHNAME(date), SUM(confirmed) , SUM(deaths)
FROM covid19_us_fact
GROUP BY UID, YEAR(date), MONTH(date), MONTHNAME(date)
ORDER BY 2, 3 DESC;

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
    LocalName VARCHAR(64),
    GovernmentForm VARCHAR(64),
    HeadOfState VARCHAR(64),
    Capital int,
    Code2 VARCHAR(3),
    PRIMARY KEY(code)
);
SELECT DISTINCT name 
FROM world.country, covid19_global_raw
WHERE world.country.name = covid19_global_raw.Country_Region
ORDER BY name
;

SELECT DISTINCT code, Name, Lat, Long_, Continent, Region, SurfaceArea, IndepYear, Population,
LifeExpectancy, GNP, LocalName, GovernmentForm, HeadOfState, Capital, Code2 
FROM world.country INNER JOIN covid19_global_raw ON
world.country.name = covid19_global_raw.Country_Region
GROUP BY code
;

SELECT Country_Region, date, SUM(confirmed), SUM(deaths)
FROM covid19_global_raw
WHERE Country_Region = 'Canada'
GROUP BY Country_Region, date
ORDER BY Country_Region
;

SELECT DISTINCT code, Country_Region, date, SUM(confirmed), SUM(deaths)
FROM  country_dim, covid19_global_raw
WHERE country_dim.Name = covid19_global_raw.Country_Region
AND Country_Region = 'Canada'
GROUP BY Country_Region, date
ORDER BY code;


CREATE TABLE IF NOT EXISTS covid19_global_fact(
    dateid bigint NOT NULL,
    country_code VARCHAR(3) NOT NULL,
    date datetime NOT NULL,
    confirmed int NOT NULL,
    deaths int NOT NULL,
    
    PRIMARY KEY(dateid, country_code),
    FOREIGN KEY (country_code) REFERENCES country_dim(code)
);
SELECT country_code, YEAR(date), MONTH(date), MONTHNAME(date), SUM(confirmed) , SUM(deaths)
FROM covid19_global_fact
GROUP BY country_code, YEAR(date), MONTH(date), MONTHNAME(date)
ORDER BY 2, 3 DESC;

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
    date_first_added datetime NULL,
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


