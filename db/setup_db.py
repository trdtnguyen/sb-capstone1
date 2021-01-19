import mysql.connector
from os import environ as env

print_traces = False
try:
    connection = mysql.connector.connect(user=env.get('MYSQL_USER'),
                                         password=env.get('MYSQL_PASSWORD'),
                                         host=env.get('MYSQL_HOST'),
                                         port=env.get('MYSQL_PORT'),
                                         database=env.get('MYSQL_DATABASE'))
    print('create tables...', end=" ")
    cursor = connection.cursor()

    sql_cmd =""
    with open('sql/create_table.sql') as f:
        lines = f.readlines()
        for line in lines:
            sql_cmd = sql_cmd + " " + line
            if ';' in line:
                if print_traces:
                    print(f'execute sql_cmd: {sql_cmd}')
                cursor.execute(sql_cmd)
                sql_cmd = ""

    print('Done.')
    print('insert data onto tables...', end=' ')
    sql_cmd =""
    with open('sql/insert_table.sql') as f:
        lines = f.readlines()
        for line in lines:
            sql_cmd = sql_cmd + " " + line
            if ';' in line:
                if print_traces:
                    print(f'execute sql_cmd: {sql_cmd}')
                cursor.execute(sql_cmd)
                sql_cmd = ""
    print('Done.')
except Exception as error:
    print("Error while connecting to database for job tracker", error)
