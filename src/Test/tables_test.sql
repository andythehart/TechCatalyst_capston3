USE DATABASE capstone_de;

-- CREATE OR REPLACE SCHEMA capstone_de.group_3_schema_Test;

USE SCHEMA capstone_de.group_3_schema_Test;


create or replace table capstone_de.group_3_schema_test.infer
SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@capstone_de.group_3_schema_test.group_3_S3_stage_yellow_green_test',
    FILE_FORMAT=>'GROUP_3_TEST_PARQUET_FORMAT'
  )
);



drop table capstone_de.group_3_schema_test.fact_green_yellow;
CREATE or replace TABLE capstone_de.group_3_schema_test.green_yellow_metadata (
    VendorID INT,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    payment_type FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    taxi_type VARCHAR,
    trip_duration FLOAT,
    --average_speed FLOAT,
    "month" INT,
    time varchar,
    hour VARCHAR,
    timeofday VARCHAR,
    year INT,
    dayofweek VARCHAR,
    "weekday/weekend" varchar,
    ehail_fee FLOAT,
    trip_type FLOAT,
    filename string
);


COPY INTO capstone_de.group_3_schema_test.green_yellow_metadata
FROM @capstone_de.group_3_schema_test.group_3_S3_stage_yellow_green_test
ON_ERROR='CONTINUE'
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
INCLUDE_METADATA = (filename = METADATA$FILENAME);


CREATE or replace TABLE capstone_de.group_3_schema_test.fact_green_yellow_test (
    VendorID NUMBER(38,0),
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    payment_type FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    taxi_type VARCHAR,
    trip_duration FLOAT,
    --average_speed FLOAT,
    "month" INT,
    time varchar,
    hour VARCHAR,
    timeofday VARCHAR,
    year INT,
    dayofweek VARCHAR,
    "weekday/weekend" varchar,
    ehail_fee FLOAT,
    trip_type FLOAT
    --filename string
);


insert into capstone_de.group_3_schema_test.fact_green_yellow_test
select 
    VendorId,
    tpep_pickup_datetime as pickup_datetime ,
    tpep_dropoff_datetime as dropoff_datetime ,
    passenger_count ,
    trip_distance ,
    RatecodeID ,
    store_and_fwd_flag ,
    PULocationID ,
    DOLocationID ,
    payment_type ,
    fare_amount ,
    extra ,
    mta_tax ,
    tip_amount ,
    tolls_amount ,
    improvement_surcharge ,
    total_amount ,
    congestion_surcharge ,
    Airport_fee ,
    REGEXP_SUBSTR(filename, 'taxi_type=([^/]+)', 1, 1, 'e', 1) as taxi_type,
    trip_duration,
    --average_speed FLOAT,
    regexp_substr(filename, 'month=(\\d{2})',1, 1, 'e', 1)as month,
    time,
    hour ,
    timeofday ,
    REGEXP_SUBSTR(filename, '(\\d{4})') AS year,
    dayofweek ,
    "weekday/weekend" as weekday_or_weekend,
    ehail_fee ,
    trip_type 

FROM GREEN_YELLOW_METADATA ; 


