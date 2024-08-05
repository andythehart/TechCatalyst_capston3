COPY INTO capstone_de.group_3_schema.subway_data
FROM @capstone_de.group_3_schema.group_3_s3_stage_subway
FILE_FORMAT = GROUP3_CSV_FORMAT
ON_ERROR='CONTINUE';
