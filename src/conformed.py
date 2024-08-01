"""
create the conformed data
"""
from configparser import ConfigParser
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, year, month, dayofweek, unix_timestamp, lit, dayofmonth


def read_aws_creds(path="aws.cfg") -> tuple[str, str]:
    """
    read in the aws credentials
    """
    config = ConfigParser()
    config.read(path)
    aws_access_key = config["AWS"]["ACCESS_KEY"]
    aws_secret_key = config["AWS"]["SECRET_KEY"]
    return aws_access_key, aws_secret_key


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


def read_df(session: SparkSession, url: str) -> DataFrame:
    """
    read data into a spark dataframe
    """
    print(f"reading df for: {url}")
    df = session.read.parquet(url)
    return df


def write_df(df: DataFrame, target_url: str, partition_by: list[str]):
    """
    write a spark df to the target url
    """
    print(f"writing dataframe to {target_url}. partitioning by: {partition_by}")
    df.write.mode("overwrite").partitionBy(partition_by).parquet(target_url)


RAW_BUCKET = "s3a://capstone-techcatalyst-raw"
CONFORMED_BUCKET = "s3a://capstone-techcatalyst-conformed/group_3"
GREEN_TAXI_URL = f"{RAW_BUCKET}/green_taxi/*.parquet"
HVFHV_URL = f"{RAW_BUCKET}/hvfhv/*.parquet"
YELLOW_TAXI_URL = f"{RAW_BUCKET}/yellow_taxi/*.parquet"

sess = create_spark_session("S3 Conformed")


def process_yellow_taxi() -> DataFrame:
    """
    process the yellow taxi data
    """
    date_col = "tpep_pickup_datetime"
    yellow_taxi_df = read_df(sess, YELLOW_TAXI_URL)
    yellow_taxi_df = yellow_taxi_df.drop_duplicates() \
        .withColumn("taxi_type", lit("yellow")) \
        .withColumn("trip_duration",
                    (unix_timestamp(col("tpep_dropoff_datetime"))
                     - unix_timestamp(col("tpep_pickup_datetime"))) / 60) \
        .withColumn("average_speed",
                    col('trip_distance') / (col('trip_duration') / 60)) \
        .withColumn("month", month(col(date_col))) \
        .withColumn("day_of_month", dayofmonth(col(date_col))) \
        .withColumn("year", year(col(date_col))) \
        .withColumn("day_of_week", dayofweek(col(date_col))) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True)
                    .otherwise(False)) \
        .withColumn("ehail_fee", lit(0)) \
        .withColumn("trip_type", lit(-1)) \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    return yellow_taxi_df


def process_green_taxi() -> DataFrame:
    """
    process the green taxi data
    """
    date_col = "lpep_pickup_datetime"
    green_taxi_df = read_df(sess, GREEN_TAXI_URL)
    green_taxi_df = green_taxi_df.drop_duplicates() \
        .withColumn("taxi_type", lit("green")) \
        .withColumn("trip_duration",
                    (unix_timestamp(col("lpep_dropoff_datetime"))
                     - unix_timestamp(col("lpep_pickup_datetime"))) / 60) \
        .withColumn("average_speed",
                    col('trip_distance') / (col('trip_duration') / 60)) \
        .withColumn("month", month(col(date_col))) \
        .withColumn("day_of_month", dayofmonth(col(date_col))) \
        .withColumn("year", year(col(date_col))) \
        .withColumn("day_of_week", dayofweek(col(date_col))) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True)
                    .otherwise(False)) \
        .withColumn("Airport_fee", lit(0)) \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    return green_taxi_df


def process_hvfhv() -> DataFrame:
    """
    process the hvfhv data
    """
    date_col = "pickup_datetime"
    hvfhv_df = read_df(sess, HVFHV_URL)
    hvfhv_df = hvfhv_df.drop_duplicates() \
        .withColumn("taxi_type", lit("hvhfv")) \
        .withColumnRenamed("trip_miles", "trip_distance") \
        .withColumn("trip_duration",
                    (unix_timestamp(col("dropoff_datetime"))
                     - unix_timestamp(col("pickup_datetime"))) / 60) \
        .withColumn("average_speed",
                    col('trip_distance') / (col('trip_duration') / 60)) \
        .withColumn("month", month(col(date_col))) \
        .withColumn("day_of_month", dayofmonth(col(date_col))) \
        .withColumn("year", year(col(date_col))) \
        .withColumn("day_of_week", dayofweek(col(date_col))) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True)
                    .otherwise(False))
    return hvfhv_df


if __name__ == '__main__':
    yellow = process_yellow_taxi()
    green = process_green_taxi()
    green_reorder = green.select(*yellow.columns) # cheap way of reordering cols
    yellow_green = yellow.union(green)
    write_df(yellow_green, f"{CONFORMED_BUCKET}/yellow_green", ["year", "month", "taxi_type"])

    hvfhv = process_hvfhv()
    write_df(hvfhv, f"{CONFORMED_BUCKET}/hvfhv", ["year", "month", "day_of_month"])
