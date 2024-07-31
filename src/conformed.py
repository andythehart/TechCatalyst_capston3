"""
create the conformed data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, year, month, dayofweek, unix_timestamp, lit, dayofmonth
from base import read_df, create_spark_session, write_df

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
                    .otherwise(False))
    return yellow_taxi_df


def process_green_taxi() -> DataFrame:
    """
    process the green taxi data
    """
    date_col = "tpep_pickup_datetime"
    green_taxi_df = read_df(sess, GREEN_TAXI_URL)
    green_taxi_df = green_taxi_df.drop_duplicates() \
        .withColumn("taxi_type", lit("green")) \
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
                    .otherwise(False))
    return green_taxi_df


def process_hvfhv() -> DataFrame:
    """
    process the hvfhv data
    """
    date_col = "pickup_datetime"
    hvfhv_df = read_df(sess, HVFHV_URL)
    hvfhv_df = hvfhv_df.drop_duplicates() \
        .withColumn("taxi_type", lit("hvhfv")) \
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
    yellow_green = yellow.union(green)
    write_df(yellow_green, CONFORMED_BUCKET, ["year", "month", "taxi_type"])

    hvfhv = process_hvfhv()
    write_df(hvfhv, CONFORMED_BUCKET, ["year", "month", "day_of_month"])
