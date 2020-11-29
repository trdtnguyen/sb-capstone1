FROM python:3.7
RUN apt-get update && apt-get -y install build-essential
RUN pip install  apache-airflow[mysql,crypto]==1.10.12 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"
RUN pip install  mysql-connector-python
RUN pip install  pandas
RUN pip install  pymysql
RUN pip install  jupyter

WORKDIR /root/airflow/

COPY setup.sh /root/airflow/setup.sh
RUN chmod +x setup.sh

COPY config.cnf /root/airflow/config.cnf
COPY dags /root/airflow/dags

COPY sql /root/airflow/sql
COPY tasks /root/airflow/tasks
COPY db /root/airflow/db
COPY test /root/airflow/test
