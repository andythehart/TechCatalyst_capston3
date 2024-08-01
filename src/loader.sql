USE DATABASE capstone_de;
USE SCHEMA group_3_schema;

CREATE TABLE yellow_green_taxi_data ();

CREATE TABLE hvfhv_data ();

COPY INTO yellow_green_taxi_data
FROM @group_3_S3_stage_yellow_green;

COPY INTO hvfhv_data ();
FROM @group_3_S3_stage_hvfhv;
