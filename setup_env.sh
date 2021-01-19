#./bin/bash
FILE_PATH=$(realpath "$0")
DIR_PATH=$(dirname $FILE_PATH)

export COVID_PROJECT_PATH=$DIR_PATH
export PYTHONPATH=$PYTHONPATH:$DIR_PATH

#export $MYSQL_DATABASE
#export MYSQL_DATABASE=ticket_event
export AIRFLOW_DATABASE=airflowdb
export MYSQL_HOST=mysql_db
export MYSQL_PORT=3306
export MYSQL_ROOT_PASSWORD=12345678
export MYSQL_USER=dtn
export MYSQL_PASSWORD=12345678
export MYSQL_ALLOW_EMPTY_PASSWORD=true
export MYSQL_ENTRYPOINT_INITDB=./mysql/docker-entrypoint-initdb.d
