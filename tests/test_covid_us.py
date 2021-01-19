import tasks
from tasks.Covid import Covid
from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil
import sys
import os

sys.path.append("/home/dtn/sb-capstone1/tasks")
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
print (sys.path)

GU = GlobalUtil.instance()
print('Test config value: ', GU.CONFIG['CORE']['PROJECT_NAME'])
spark = SparkSession \
    .builder \
    .appName("TestApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

covid = Covid(spark)

RAW_TABLE_NAME = 'covid19_us_raw'
DIM_TABLE_NAME = 'covid19_us_dim'
FACT_TABLE_NAME = 'covid19_us_fact'
MONTHLY_FACT_TABLE_NAME = 'covid19_us_monthly_fact'


def test_extract_us():
    # extract data to raw table
    covid.extract_us()
    # read latest_data table to confirm
    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, RAW_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)


#test_extract_us()


# def test_transform_raw_to_fact_us():
#     covid.transform_raw_to_fact_us()
#
#     # read latest_data table to confirm
#     latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, FACT_TABLE_NAME)
#     assert (is_resume_extract is not None)
#     assert is_resume_extract
#     assert (latest_date > GU.START_DEFAULT_DATE)
#
#
# def test_aggregate_fact_to_monthly_fact_us():
#     covid.aggregate_fact_to_monthly_fact_us()
#     assert True
#     # read latest_data table to confirm
#     latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, MONTHLY_FACT_TABLE_NAME)
#     assert (is_resume_extract is not None)
#     assert is_resume_extract
#     assert (latest_date > GU.START_DEFAULT_DATE)
