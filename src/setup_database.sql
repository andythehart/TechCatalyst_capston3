USE DATABASE capstone_de;

CREATE OR REPLACE SCHEMA capstone_de.group_3_schema;

USE SCHEMA capstone_de.group_3_schema;

CREATE OR REPLACE FILE FORMAT capstone_de.group_3_schema.group_3_parquet
    TYPE = PARQUET;

CREATE OR REPLACE FILE FORMAT group3_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_S3_stage_yellow_green
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/yellow_green'
    FILE_FORMAT = group_3_parquet;

CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_S3_stage_hvfhv
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/hvfhv';
    FILE_FORMAT = group_3_parquet;

CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_S3_stage_borough_pricing
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/borough_hour_pricing/'
    FILE_FORMAT = group3_csv_format;