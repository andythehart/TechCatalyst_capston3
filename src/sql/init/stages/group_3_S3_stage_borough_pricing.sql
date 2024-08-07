CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_S3_stage_borough_pricing
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/borough_hour_pricing/'
    FILE_FORMAT = group3_csv_format;
