from tasks.Covid import Covid
from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil

GU = GlobalUtil.instance()
print('Test config value: ', GU.CONFIG['CORE']['PROJECT_NAME'])
spark = SparkSession \
    .builder \
    .appName("TestApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

covid = Covid(spark)

RAW_TABLE_NAME = 'covid19_global_raw'
DIM_TABLE_NAME = 'country_dim'
FACT_TABLE_NAME = 'covid19_global_fact'
COUNTRY_TABLE_NAME = 'world_country'
MONTHLY_FACT_TABLE_NAME = 'covid19_global_monthly_fact'


def test_transform_raw_to_dim_country():
    covid.transform_raw_to_dim_country()
    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, DIM_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert is_resume_extract
    assert (latest_date > GU.START_DEFAULT_DATE)

    # Read after write
    dim_df = GU.read_from_db(spark, DIM_TABLE_NAME)
    assert dim_df.count() > 0
