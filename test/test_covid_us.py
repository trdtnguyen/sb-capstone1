from tasks.Covid import Covid
from pyspark.sql import SparkSession
from tasks.GlobalUtil import GlobalUtil


GU = GlobalUtil.instance()
spark = SparkSession \
    .builder \
    .appName(GU.CONFIG['CORE']['PROJECT_NAME']) \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

covid = Covid(spark)

RAW_TABLE_NAME = 'covid19_us_raw'
DIM_TABLE_NAME = 'covid19_us_dim'


def test_extract_us():
    # extract data to raw table
    covid.extract_us()
    # read latest_data table to confirm
    latest_df, is_resume_extract, latest_date = GU.read_latest_data(spark, RAW_TABLE_NAME)
    assert (is_resume_extract is not None)
    assert (is_resume_extract)
    assert(latest_date > GU.START_DEFAULT_DATE)
