USE DATABASE capstone_de;

CREATE OR REPLACE SCHEMA group_3_schema;

USE SCHEMA group_3_schema;

CREATE OR REPLACE FILE FORMAT group_3_parquet
    TYPE = PARQUET;

CREATE OR REPLACE STAGE group_3_S3_stage_yellow_green
    STORAGE_INTEGRTATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/yellow_green'
    FILE_FORMAT = group_3_parquet

CREATE OR REPLACE STAGE group_3_S3_stage_hvfhv
    STORAGE_INTEGRTATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/hvfhv';
