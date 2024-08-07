COPY INTO capstone_de.group_3_schema.CRASH_DATA
FROM @capstone_de.group_3_schema.group_3_S3_stage_crashdata
ON_ERROR='CONTINUE'
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';
