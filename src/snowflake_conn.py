"""
snowflake connector code
"""
import snowflake.connector
from snowflake.connector import SnowflakeConnection


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


def run_sql(conn: SnowflakeConnection, sql: str):
    """
    run a sql statement
    """
    conn.cursor().execute(sql)


def copy_into(conn: SnowflakeConnection, sql: str, aws_access_key: str, aws_secret_key: str):
    """
    run copy into command
    """
    conn.cursor().execute(
        sql.format(
            aws_access_key_id = aws_access_key,
            aws_secret_access_key = aws_secret_key
        )
    )
