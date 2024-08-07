CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_s3_stage_subway
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/subway_data'
    FILE_FORMAT = GROUP3_CSV_FORMAT;
