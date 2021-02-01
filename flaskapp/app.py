"""
app_bk2.py
"""
from flask import Flask, jsonify, request, render_template, redirect, url_for

import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, explode, struct, lit, udf, when
from flask_cors import CORS, cross_origin
from tasks.GlobalUtil import GlobalUtil
from tasks.Covid import Covid
from datetime import datetime, timedelta

### For flask
from flask import Flask, render_template, send_from_directory
from waitress import serve
#from flaskr.data import data_loader
from data import data_loader

GU = GlobalUtil.instance()
PROJECT_PATH = os.path.join(os.path.dirname(__file__), "..")
CONFIG = configparser.RawConfigParser()  # Use RawConfigParser() to read url with % character
CONFIG_FILE = 'config.cnf'
config_path = os.path.join(PROJECT_PATH, CONFIG_FILE)
CONFIG.read(config_path)

spark = SparkSession \
    .builder \
    .appName(CONFIG['CORE']['PROJECT_NAME']) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

covid = Covid(spark)
# Prefetch database
COVID_STOCK_FACT_TABLE_NAME = GU.CONFIG['DATABASE']['COVID_STOCK_FACT_TABLE_NAME']
covid_stock_fact_df = GU.read_from_db(spark, COVID_STOCK_FACT_TABLE_NAME)
print('========================')
print("API server is ready.")
# Flask
app = Flask(__name__)
# Solve the error: CORS header ‘Access-Control-Allow-Origin’ missing
# app.config['SECRET_KEY'] = 'the quick brown fox jumps over the lazy   dog'
# app.config['CORS_HEADERS'] = 'Content-Type'
#
# cors = CORS(app, resources={r"/": {"origins": f"http://{CONFIG['API']['API_HOST']}:{CONFIG['API']['API_PORT']}"},
#                             r"/query": {"origins": f"http://{CONFIG['API']['API_HOST']}:{CONFIG['API']['API_PORT']}"}
#                             })

# @app.route('/foo', methods=['POST'])
# @cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
# def foo():
#     return request.json['inputVar']

# load the data sets from the covid_19_data.csv
# data_loader_obj = data_loader.DataLoader()
# DATA_SET_FULL = data_loader_obj.prepare_data_set_full()
# DATA_SET_GROUPED = data_loader_obj.prepare_data_set_grouped()


@app.route('/favicon.ico')
def favicon():
    """
    function to properly handle favicon
    :return:
    """
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/', methods=['GET'])
# @cross_origin(origin='192.168.0.2',headers=['Content- Type','Authorization'])
def index():
    return render_template('index.html')


@app.route('/query_covid_stock_chart', methods=['GET'])
# @cross_origin(origin='192.168.0.2',headers=['Content- Type','Authorization'])
def main():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        latest_df, is_resume_extract, latest_date = \
            GU.read_latest_data(spark, COVID_STOCK_FACT_TABLE_NAME)
        # validation input arguments
        if latest_date < end_date:
            end_date = latest_date

        result_df = covid_stock_fact_df \
            .filter((col('date') >= start_date) & (col('date') <= end_date)
                    )
        # result_df.show()
        # result_json = result_df.toJSON().collect()
        # return jsonify(result_json), 200

        date_arr = result_df.select(col('date')).rdd.flatMap(lambda x: x).collect()
        #convert from date to str
        date_arr = [date.strftime('%Y-%m-%d') for date in date_arr]
        us_confirmed_arr = result_df.select(col('us_confirmed')).rdd.flatMap(lambda x: x).collect()
        us_death_arr = result_df.select(col('us_deaths')).rdd.flatMap(lambda x: x).collect()
        global_confirmed_arr = result_df.select(col('global_confirmed')).rdd.flatMap(lambda x: x).collect()
        global_death_arr = result_df.select(col('global_deaths')).rdd.flatMap(lambda x: x).collect()
        sp500_arr = result_df.select(col('sp500_score')).rdd.flatMap(lambda x: x).collect()
        nasdaq100_arr = result_df.select(col('nasdaq100_score')).rdd.flatMap(lambda x: x).collect()
        dowjones_arr = result_df.select(col('dowjones_score')).rdd.flatMap(lambda x: x).collect()

        return render_template('covid_stock_chart.html', template_labels=date_arr,
                               arg_us_confirmed_arr=us_confirmed_arr,
                               arg_us_death_arr=us_death_arr,
                               arg_global_confirmed_arr=global_confirmed_arr,
                               arg_global_death_arr=global_death_arr,
                               arg_sp500_arr=sp500_arr,
                               arg_nasdaq100_arr=nasdaq100_arr,
                               arg_dowjones_arr=dowjones_arr,
                               )
    except ValueError as e:
        print(e)
        return jsonify(message="Incorrect data format, should be YYYY-MM-DD",
                       status=500)

@app.route('/get_file/<string:filename>', methods=['GET'])
def get_file(filename):
    return send_from_directory(app.root_path, filename)



@app.route('/<string:item>', methods=['GET'])
def get_item_details(item):
    """
    the route for each "drilldown" item details
    :param item:
    :return:
    """
    filtered_data_set = [x for x in DATA_SET_FULL if x.get('ObservationDate') == item]

    return render_template('details.html', template_data_set=filtered_data_set)


if __name__ == "__main__":
    # serve(APP, host='192.168.0.2', port=5001, threads=4)
    app.run(host=CONFIG['API']['API_HOST'], port=CONFIG['API']['API_PORT'])
