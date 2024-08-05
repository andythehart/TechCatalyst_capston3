CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_S3_stage_yellow_green
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/yellow_green'
    FILE_FORMAT = group_3_parquet;
