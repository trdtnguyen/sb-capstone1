#./bin/bash
DAG1=covid19_us_handle
DAG1_TASK1=extract_us
DAG1_TASK2=transform_raw_to_fact_us
DAG1_TASK3=aggregate_fact_to_monthly_fact_us

DAG2=covid19_global_handle
DAG2_TASK1=transform_raw_to_dim_country
DAG2_TASK2=extract_global
DAG2_TASK3=transform_raw_to_fact_global
DAG2_TASK4=aggregate_fact_to_monthly_fact_global

DAG3=stock_handle
DAG3_TASK1=extract_sp500_tickers
DAG3_TASK2=extract_batch_stock
DAG3_TASK3=transform_raw_to_fact_stock
DAG3_TASK4=aggregate_fact_to_monthly_fact_stock


TIME=now

airflow test ${DAG1} ${DAG1_TASK1} ${TIME}
airflow test ${DAG1} ${DAG1_TASK2} ${TIME}
airflow test ${DAG1} ${DAG1_TASK3} ${TIME}

airflow test ${DAG2} ${DAG2_TASK1} ${TIME}
airflow test ${DAG2} ${DAG2_TASK2} ${TIME}
airflow test ${DAG2} ${DAG2_TASK3} ${TIME}
airflow test ${DAG2} ${DAG2_TASK4} ${TIME}

airflow test ${DAG3} ${DAG3_TASK1} ${TIME}
airflow test ${DAG3} ${DAG3_TASK2} ${TIME}
airflow test ${DAG3} ${DAG3_TASK3} ${TIME}
airflow test ${DAG3} ${DAG3_TASK4} ${TIME}
