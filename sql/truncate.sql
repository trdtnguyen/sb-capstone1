SET FOREIGN_KEY_CHECKS=0;
TRUNCATE covid19_us_monthly_fact;
TRUNCATE covid19_us_dim;
TRUNCATE covid19_us_fact;
TRUNCATE covid19_global_monthly_fact;
TRUNCATE covid19_global_fact;
TRUNCATE country_dim;
TRUNCATE covid19_global_raw;
TRUNCATE covid19_us_raw;

TRUNCATE stock_price_monthly_fact;
TRUNCATE stock_price_fact;
TRUNCATE stock_ticker_raw;
TRUNCATE stock_price_raw;

TRUNCATE bol_series_fact;
TRUNCATE bol_series_dim;
TRUNCATE bol_raw;
SET FOREIGN_KEY_CHECKS=1;
