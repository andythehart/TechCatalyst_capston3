# %pip install snowflake-connector-python # run this to install the snowflake connector in databricks
"""
the entire end to end pipeline configured for a databricks notebook
"""
from configparser import ConfigParser
from types import FunctionType
import time
import logging
import logging.config
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, when, year, month, unix_timestamp, lit, dayofmonth, date_format, avg
import snowflake.connector
from snowflake.connector import SnowflakeConnection
logging.config.dictConfig({'version': 1, 'disable_existing_loggers': True})
logger = logging.getLogger(__name__)
# ****************************************************** User defined variables - must set these before running
ENVIRONMENT = "dev"  # or "prod"

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_ROLE = ""
SNOWFLAKE_ACCT = ""
SNOWFLAKE_WAREHOUSE = ""
SNOWFLAKE_DB = ""
SNOWFLAKE_SCHEMA = ""

# ************************************************************ Setup logger
LOG_FILE_NAME = f"run_{ENVIRONMENT}_{time.time()}.log"
logging.basicConfig(filename=LOG_FILE_NAME, encoding='utf-8', level=logging.INFO)

# ************************************************************ Constants
RAW_BUCKET = "s3a://capstone-techcatalyst-raw"
CONFORMED_BUCKET = "s3a://capstone-techcatalyst-conformed/group_3"
TRANSFORMED_BUCKET = "s3a://capstone-techcatalyst-transformed/group_3"

GREEN_TAXI_URL = f"{RAW_BUCKET}/green_taxi/*.parquet"
YELLOW_TAXI_URL = f"{RAW_BUCKET}/yellow_taxi/*.parquet"
YELLOW_GREEN_URL = f"{CONFORMED_BUCKET}/yellow_green"


# ************************************************************ Helper Functions
def timefunc(f: FunctionType):
    """
    wrapper func for timing functions
    """
    def timed(*args, **kwargs):
        time_start = time.time()
        results = f(*args, **kwargs)
        time_end = time.time()
        total_time = time_end - time_start
        logger.info(f"function:{f.__name__} with args: {args}, {kwargs} took: {total_time}")
        return results
    return timed


def read_aws_creds(path="") -> tuple[str, str]:
    """
    read in the aws credentials
    """
    # for databricks set your access key above
    if path != "":
        config = ConfigParser()
        config.read(path)
        access_key = config["AWS"]["ACCESS_KEY"]
        secret_key = config["AWS"]["SECRET_KEY"]
        return access_key, secret_key
    else:
        access_key = AWS_ACCESS_KEY
        secret_key = AWS_SECRET_KEY
        return access_key, secret_key


@timefunc
def create_spark_session(name: str) -> SparkSession:
    """
    create a spark session
    """
    access_key, secret_key = read_aws_creds()
    spark = SparkSession.builder  \
        .appName(name) \
        .config("fs.s3a.access.key", access_key) \
        .config("fs.s3a.secret.key", secret_key) \
        .getOrCreate()
    logger.info("spark session created")
    return spark


@timefunc
def read_df(session: SparkSession, url: str) -> DataFrame:
    """
    read data into a spark dataframe
    """
    logger.info(f"reading df for: {url}")
    df = session.read.parquet(url)
    return df


@timefunc
def write_df(df: DataFrame, target_url: str, partition_by: list[str]):
    """
    write a spark df to the target url
    """
    logger.info(f"writing dataframe to {target_url}. partitioning by: {partition_by}")
    df.write.mode("overwrite").partitionBy(partition_by).parquet(target_url)


@timefunc
def write_file(session: SparkSession, file_path: str, target_url: str):
    """
    write a file in text form
    """
    with open(file_path, 'r') as f:
        txt = f.read()
        rdd = session.sparkContext.parallelize(txt.split('\n'))
        df = rdd.map(lambda line: Row(value=line)).toDF().repartition(1)
        df.write.mode("overwrite").text(target_url)


@timefunc
def create_snowflake_connection(user: str, password: str, acct: str, warehouse: str, db: str, schema: str, role: str) -> SnowflakeConnection:
    """
    create a snowflake connection
    """
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        role=role,
        account=acct,
        warehouse=warehouse,
        database=db,
        schema=schema
    )
    return conn


@timefunc
def run_sql(conn: SnowflakeConnection, sql: str):
    """
    run a sql statement
    """
    logger.info("running sql: " + sql)
    conn.cursor().execute(sql)


@timefunc
def copy_into(conn: SnowflakeConnection, sql: str, aws_access_key: str, aws_secret_key: str):
    """
    run copy into command
    """
    logger.info("running copy into: " + sql)
    conn.cursor().execute(
        sql.format(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
    )


# ************************************************************ Processing
@timefunc
def process_yellow_taxi(sess: SparkSession) -> DataFrame:
    """
    process the yellow taxi data
    """
    date_col = "pickup_datetime"
    yellow_taxi_df = read_df(sess, YELLOW_TAXI_URL)
    yellow_taxi_df = yellow_taxi_df.drop_duplicates() \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumn("taxi_type", lit("yellow")) \
        .withColumn("trip_duration",
                    (unix_timestamp(col("dropoff_datetime"))
                     - unix_timestamp(col("pickup_datetime"))) / 60) \
        .withColumn("average_speed",
                    col('trip_distance') / (col('trip_duration') / 60)) \
        .withColumn("month", month(col(date_col))) \
        .withColumns({'time': date_format(col(date_col), 'HH:mm:ss'), 'hour': date_format(col(date_col), 'HH')})\
        .withColumn('timeofday', when(date_format(col(date_col), 'HH')<12, 'Morning').when(date_format(col(date_col), 'HH').between(12,17),'Afternoon').otherwise('Evening')) \
        .withColumn("day_of_month", dayofmonth(col(date_col))) \
        .withColumn("year", year(col(date_col))) \
        .withColumn("day_of_week", date_format(col(date_col), 'EEE')) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True)
                    .otherwise(False)) \
        .withColumn("ehail_fee", lit(0)) \
        .withColumn("trip_type", lit(-1))
    return yellow_taxi_df


@timefunc
def process_green_taxi(sess: SparkSession) -> DataFrame:
    """
    process the green taxi data
    """
    date_col = "pickup_datetime"
    green_taxi_df = read_df(sess, GREEN_TAXI_URL)
    green_taxi_df = green_taxi_df.drop_duplicates() \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumn("taxi_type", lit("green")) \
        .withColumn("trip_duration",
                    (unix_timestamp(col("dropoff_datetime"))
                     - unix_timestamp(col("pickup_datetime"))) / 60) \
        .withColumn("average_speed",
                    col('trip_distance') / (col('trip_duration') / 60)) \
        .withColumn("month", month(col(date_col))) \
        .withColumns({'time': date_format(col(date_col), 'HH:mm:ss'), 'hour': date_format(col(date_col), 'HH')})\
        .withColumn('timeofday', when(date_format(col(date_col), 'HH')<12, 'Morning').when(date_format(col(date_col), 'HH').between(12,17),'Afternoon').otherwise('Evening')) \
        .withColumn("day_of_month", dayofmonth(col(date_col))) \
        .withColumn("year", year(col(date_col))) \
        .withColumn("day_of_week", date_format(col(date_col), 'EEE')) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True)
                    .otherwise(False)) \
        .withColumn("Airport_fee", lit(0))
    return green_taxi_df


@timefunc
def combine_yellow_green(y: DataFrame, g: DataFrame) -> DataFrame:
    """
    combine yellow and green
    """
    g_r = g.select(y.columns)  # cheap way of reordering cols
    return y.union(g_r)


@timefunc
def process_yellow_green(sess: SparkSession) -> [DataFrame, DataFrame]:
    """
    process conformed yellow/green and produce outliers report
    """
    yg_df = read_df(sess, YELLOW_GREEN_URL)

    # a fare amount of >150 is considered a flag for fraud
    outliers_report = yg_df.filter((col("fare_amount") > 150) | (col("trip_distance") >= 60))

    # filter outlier trip distances: 60 is the 99.99 percentile
    yg_df = yg_df.filter(col("trip_distance") < 60)
    # we replace 0 valued distance with the average distance per business request
    average_distance = yg_df.filter(col("trip_distance") != 0).agg(avg(col("trip_distance"))).collect()[0][0]
    yg_df = yg_df.withColumn("trip_distance", when(col("trip_distance") == 0, average_distance).otherwise(col("trip_distance"))) \
        .withColumn("passenger_count", when((col("passenger_count") == 0) | (col("passenger_count").isNull()), lit(1)).otherwise(col("passenger_count")))
    return yg_df, outliers_report


@timefunc
def main():
    if ENVIRONMENT != "dev" and ENVIRONMENT != "prod":
        logger.info("must specify environment either dev or prod")
        return

    logger.info(f"********** Start Job in {ENVIRONMENT} ***********")
    spark_sess = create_spark_session("End to End")
    snow_conn = create_snowflake_connection(SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DB, SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE)

    # conformed step
    yellow = process_yellow_taxi(spark_sess)
    green = process_green_taxi(spark_sess)
    yellow_green = combine_yellow_green(yellow, green)
    write_df(yellow_green, f"{CONFORMED_BUCKET}/yellow_green", ["year", "month", "taxi_type"])

    # transformed step
    yg_transformed, outliers_report = process_yellow_green(spark_sess)
    write_df(yg_transformed, f"{TRANSFORMED_BUCKET}/yellow_green", [])
    write_df(outliers_report, f"{TRANSFORMED_BUCKET}/outlier_report", [])

    # All data is now in the transformed bucket; time to move it to snowflake
    # this script assumes the tables/stages already exist

    # clean the table
    base = "capstone_de.group_3_schema"
    if ENVIRONMENT == "dev":
        fact_tbl = f"{base}.dev_fact_green_yellow"
        outlier_tbl = f"{base}.dev_outliers_report"
    else:  # prod mode
        fact_tbl = f"{base}.fact_green_yellow"
        outlier_tbl = f"{base}.outliers_report"

    # clean out the old data
    run_sql(snow_conn, f"TRUNCATE TABLE {fact_tbl}")
    run_sql(snow_conn, f"TRUNCATE TABLE {outlier_tbl}")

    # the copy into
    sql = f"""COPY INTO {fact_tbl}
    FROM @capstone_de.group_3_schema.group_3_S3_stage_yellow_green
    ON_ERROR='CONTINUE'
    MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';
    """
    copy_into(snow_conn, sql, AWS_ACCESS_KEY, AWS_SECRET_KEY)

    sql = f"""COPY INTO {outlier_tbl}
    FROM @capstone_de.group_3_schema.group_3_S3_stage_outlier_report
    ON_ERROR='CONTINUE'
    MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';
    """
    copy_into(snow_conn, sql, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    logger.info(f"********** Finish Job in {ENVIRONMENT} ***********")
    print("writing log...")
    write_file(spark_sess, LOG_FILE_NAME, f"{TRANSFORMED_BUCKET}/logs/{LOG_FILE_NAME}")


if __name__ == '__main__':
    main()
