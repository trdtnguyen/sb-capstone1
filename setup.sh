INIT_FILE=.airflowinitialized
if [ ! -f $INIT_FILE ]; then
    # Create all Airflow configuration files
    #airflow initdb
    #rm /root/airflow/airflow.db

    # Secure the storage of connections’ passwords
    #python fernet.py

    # Wait until the DB is ready
    apt update && apt install -y netcat
    echo "wait mysql on 3306 to open ..."
    while ! nc -z mysql_db 3306; do
        sleep 10
    done
    apt remove -y netcat

    # Setup the DB for airflow
    #python mysqlconnect.py
    airflow initdb

    # Setup app DB
    python db/setup_db.py

    #start jupyter notebook
    jupyter notebook --ip=0.0.0.0 --port=8080 --allow-root
    # This configuration is done only the first time
    touch $INIT_FILE
fi

# Run the Airflow webserver and scheduler. Disable this to run manually
#airflow scheduler &
#airflow webserver &
#wait

#Disable this to use airflow scheduler
tail -f /dev/null
