FROM python:3.8

ARG PROJECT_HOME=/root/airflow

WORKDIR ${PROJECT_HOME}/

#### Airflow
RUN apt-get update && apt-get -y install build-essential
RUN pip install  apache-airflow[mysql,crypto]==1.10.12 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"

#### Spark
ARG SPARK_NAME=spark-3.0.1-bin-hadoop2.7
ARG SPARK_EXT_ZIP=tgz
ARG SPARK_FILE_NAME=${SPARK_NAME}.${SPARK_EXT_ZIP}

RUN wget https://mirrors.sonic.net/apache/spark/spark-3.0.1/${SPARK_FILE_NAME}
RUN tar -xvzf ${SPARK_FILE_NAME} 
# for jdbc
#RUN cp jars/* ${SPARK_NANE}/jars/
ENV SPARK_HOME ${SPARK_NAME}
RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

#### Mysql
RUN pip install  mysql-connector-python
RUN pip install  pymysql

# Core project
RUN pip install  pyspark
RUN pip install  pandas
RUN pip install  pandas-datareader
RUN pip install  jupyter
RUN pip install  bs4
RUN pip install  sqlalchemy

COPY setup.sh /root/airflow/setup.sh
RUN chmod +x setup.sh

COPY config.cnf config.cnf
COPY dags dags
COPY sql sql
COPY tasks tasks
COPY db db
COPY data data
COPY test test

COPY jars jars
COPY jars/* spark-3.0.1-bin-hadoop2.7/jars/ 
RUN cp jars/* spark-3.0.1-bin-hadoop2.7/jars/

###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME
