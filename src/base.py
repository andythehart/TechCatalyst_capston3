""""
basic functions and constants for the project
"""
from configparser import ConfigParser
from pyspark.sql import SparkSession, DataFrame


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
    return spark


def read_df(session: SparkSession, url: str) -> DataFrame:
    """
    read data into a spark dataframe
    """
    df = session.read.parquet(url)
    return df


def write_df(df: DataFrame, target_url: str, partition_by: list[str]):
    """
    write a spark df to the target url
    """
    df.write.mode("overwrite").partitionBy(partition_by).parquet(target_url)
