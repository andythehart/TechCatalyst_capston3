USE DATABASE capstone_de;
USE SCHEMA capstone_de.group_3_schema;

COPY INTO capstone_de.group_3_schema.yellow_green_taxi_data
FROM @capstone_de.group_3_schema.group_3_S3_stage_yellow_green;

COPY INTO capstone_de.group_3_schema.hvfhv_data ();
FROM @capstone_de.group_3_schema.group_3_S3_stage_hvfhv;
