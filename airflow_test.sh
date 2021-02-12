#./bin/bash
DAG_ID=covid19cor_etl

COVID_US_TASK1=extract_us
COVID_US_TASK2=transform_raw_to_fact_us
COVID_US_TASK3=aggregate_fact_to_monthly_fact_us

COVID_GLOBAL_TASK1=transform_raw_to_dim_country
COVID_GLOBAL_TASK2=extract_global
COVID_GLOBAL_TASK3=transform_raw_to_fact_global
COVID_GLOBAL_TASK4=aggregate_fact_to_monthly_fact_global

COVID_AGGREGATE_TASK1=aggregate_fact_to_sum_fact
COVID_AGGREGATE_TASK2=aggregate_fact_to_sum_monthly_fact

STOCK_TASK1=extract_major_stock_indexes
STOCK_TASK2=aggregate_fact_to_monthly_fact_stock_index

BOL_TASK1=extract_BOL

CONSOLIDATE_TASK1=consolidate_covid_stock
CONSOLIDATE_TASK2=aggregate_covid_stock_monthly_fact
CONSOLIDATE_TASK3=aggregate_covid_stock_monthly_fact

TIME=now

airflow test ${DAG_ID} ${COVID_US_TASK1} ${TIME}
airflow test ${DAG_ID} ${COVID_US_TASK2} ${TIME}
airflow test ${DAG_ID} ${COVID_US_TASK3} ${TIME}

airflow test ${DAG_ID} ${COVID_GLOBAL_TASK1} ${TIME}
airflow test ${DAG_ID} ${COVID_GLOBAL_TASK2} ${TIME}
airflow test ${DAG_ID} ${COVID_GLOBAL_TASK3} ${TIME}
airflow test ${DAG_ID} ${COVID_GLOBAL_TASK4} ${TIME}

airflow test ${DAG_ID} ${COVID_AGGREGATE_TASK1} ${TIME}
airflow test ${DAG_ID} ${COVID_AGGREGATE_TASK2} ${TIME}

airflow test ${DAG_ID} ${STOCK_TASK1} ${TIME}
airflow test ${DAG_ID} ${STOCK_TASK2} ${TIME}

airflow test ${DAG_ID} ${BOL_TASK1} ${TIME}

airflow test ${DAG_ID} ${CONSOLIDATE_TASK1} ${TIME}
airflow test ${DAG_ID} ${CONSOLIDATE_TASK2} ${TIME}
airflow test ${DAG_ID} ${CONSOLIDATE_TASK3} ${TIME}
