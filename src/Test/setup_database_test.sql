USE DATABASE capstone_de;

CREATE OR REPLACE SCHEMA capstone_de.group_3_schema_Test;

USE SCHEMA capstone_de.group_3_schema_Test;

CREATE OR REPLACE FILE FORMAT group_3_test_parquet_format
TYPE = 'PARQUET';

drop stage capstone_de.group_3_schema.group_3_S3_stage_yellow_green_test;
CREATE OR REPLACE STAGE capstone_de.group_3_schema_test.group_3_S3_stage_yellow_green_test
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3_test/yellow_green_test'
    FILE_FORMAT = GROUP_3_TEST_PARQUET_FORMAT;


