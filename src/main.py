"""
the entire end to end pipeline configured for a databricks notebook
"""
from configparser import ConfigParser
from types import FunctionType
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, year, month, dayofweek, unix_timestamp, lit, dayofmonth, date_format
import snowflake.connector
from snowflake.connector import SnowflakeConnection

# ****************************************************** User defined variables
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_ACCT = ""
SNOWFLAKE_WAREHOUSE = ""
SNOWFLAKE_DB = ""
SNOWFLAKE_SCHEMA = ""
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
        print(f"function:{f.__name__} with args: {args}, {kwargs} took: {total_time}")
        return results
    return timed


def read_aws_creds(path="") -> tuple[str, str]:
    """
    read in the aws credentials
    """
    # for databricks set your access key above
    if len(path) != "":
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
    print("spark session created")
    return spark


@timefunc
def read_df(session: SparkSession, url: str) -> DataFrame:
    """
    read data into a spark dataframe
    """
    print(f"reading df for: {url}")
    df = session.read.parquet(url)
    return df


@timefunc
def write_df(df: DataFrame, target_url: str, partition_by: list[str]):
    """
    write a spark df to the target url
    """
    print(f"writing dataframe to {target_url}. partitioning by: {partition_by}")
    df.write.mode("overwrite").partitionBy(partition_by).parquet(target_url)


def create_snowflake_connection(user: str, password: str, acct: str, warehouse: str, db: str, schema: str) -> SnowflakeConnection:
    """
    create a snowflake connection
    """
    conn = snowflake.connector.connect(
        user=user,
        password=password,
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
    conn.cursor().execute(sql)


@timefunc
def copy_into(conn: SnowflakeConnection, sql: str, aws_access_key: str, aws_secret_key: str):
    """
    run copy into command
    """
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
def process_yellow_green(sess: SparkSession) -> DataFrame:
    """
    process conformed yellow/green
    """
    yg_df = read_df(sess, YELLOW_GREEN_URL)
    return yg_df


@timefunc
def main():
    spark_sess = create_spark_session("End to End")
    snow_conn = create_snowflake_connection()
    # conformed step
    yellow = process_yellow_taxi(spark_sess)
    green = process_green_taxi(spark_sess)
    yellow_green = combine_yellow_green(yellow, green)
    write_df(yellow_green, f"{CONFORMED_BUCKET}/yellow_green", ["year", "month", "taxi_type"])

    # transformed step
    yg_transformed = process_yellow_green(spark_sess)
    write_df(yg_transformed, f"{TRANSFORMED_BUCKET}/yellow_green", [])

    # All data is now in the transformed bucket; time to move it to snowflake
    # this script assumes the tables already exist
    
    # clean the table
    run_sql(snow_conn, "TRUNCATE TABLE capstone_de.group_3_schema.fact_green_yellow")

    # the copy into
    sql = """COPY INTO capstone_de.group_3_schema.fact_green_yellow
    FROM @capstone_de.group_3_schema.group_3_S3_stage_yellow_green
    ON_ERROR='CONTINUE'
    MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';
    """
    copy_into(snow_conn, sql, AWS_ACCESS_KEY, AWS_SECRET_KEY)


if __name__ == '__main__':
    main()
