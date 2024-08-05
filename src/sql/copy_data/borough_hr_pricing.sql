COPY INTO capstone_de.group_3_schema.BOROUGH_HR_PRICING
FROM @capstone_de.group_3_schema.group_3_S3_stage_borough_pricing
ON_ERROR='CONTINUE';
