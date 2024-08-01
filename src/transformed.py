"""
transform the conformed data
"""
from configparser import ConfigParser
from types import FunctionType
import time
from pyspark.sql import SparkSession, DataFrame


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


def read_aws_creds(path="aws.cfg") -> tuple[str, str]:
    """
    read in the aws credentials
    """
    config = ConfigParser()
    config.read(path)
    aws_access_key = config["AWS"]["ACCESS_KEY"]
    aws_secret_key = config["AWS"]["SECRET_KEY"]
    return aws_access_key, aws_secret_key


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
    return


CONFORMED_BUCKET = "s3a://capstone-techcatalyst-conformed/group_3"
HVFHV_URL = f"{CONFORMED_BUCKET}/hvfhv/*.parquet"
YELLOW_GREEN_URL = f"{CONFORMED_BUCKET}/yellow_green"
TRANSFORMED_BUCKET = "s3a://capstone-techcatalyst-transformed/group_3"

sess = create_spark_session("S3 Transformed")


@timefunc
def process_yellow_green() -> DataFrame:
    """
    process conformed yellow/green
    """
    yg_df = read_df(sess, YELLOW_GREEN_URL)
    return yg_df


@timefunc
def process_hvfhv() -> DataFrame:
    """
    process conformed hvfhv
    """
    hvfhv_df = read_df(sess, HVFHV_URL)
    return hvfhv_df


if __name__ == '__main__':
    yg_transformed = process_yellow_green()
    write_df(yg_transformed, f"{TRANSFORMED_BUCKET}/yellow_green", ["year", "month", "taxi_type"])

    hvfhv = process_hvfhv()
    write_df(hvfhv, f"{TRANSFORMED_BUCKET}/hvfhv", ["year", "month", "day_of_month"])
