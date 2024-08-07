COPY INTO capstone_de.group_3_schema.fact_green_yellow
FROM @capstone_de.group_3_schema.group_3_S3_stage_yellow_green
ON_ERROR='CONTINUE'
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';
