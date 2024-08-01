USE DATABASE capstone_de;
USE SCHEMA capstone_de.group_3_schema;

CREATE TABLE capstone_de.group_3_schema.fact_green_yellow (
    VendorID INT,
    pickup_datetime DATETIME,
    dropoff_datetime DATETIME,
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
    average_speed FLOAT,
    month INT,
    time VARCHAR,
    hour VARCHAR,
    timeofday VARCHAR,
    day_of_month INT,
    year INT,
    day_of_week INT,
    is_weekend boolean,
    ehail_fee FLOAT,
    trip_type FLOAT
);

CREATE TABLE capstone_de.group_3_schema.fact_hvhfv (
-- todo?
);

-- Dim tables
CREATE TABLE capstone_de.group_3_schema.dim_hvfhs_license_num (
    id VARCHAR,
    licensee VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_hvfhs_license_num
VALUES
('HV0002', 'Juno'),
('HV0003', 'Uber'),
('HV0004', 'Via'),
('HV0005', 'Lyft');

CREATE TABLE capstone_de.group_3_schema.dim_ratecode (
    id INT,
    ratecode VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_ratecode (id, ratecode)
VALUES
(1, 'Standard Rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated Fare'),
(6, 'Group Ride');

CREATE TABLE capstone_de.group_3_schema.dim_payment_type (
    id INT,
    payment_type VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_payment_type (id, payment_type)
VALUES
(1, 'Credit Card'),
(2, 'Cash'),
(3, 'No Charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided Trip');

CREATE TABLE capstone_de.group_3_schema.dim_store_fwd_flag (
    y_n VARCHAR,
    s VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_store_fwd_flag (y_n, s)
VALUES
('Y', 'store and forward trip'),
('N', 'not a store and forward trip');

CREATE TABLE capstone_de.group_3_schema.dim_trip_type (
    id INT,
    trip_type VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_trip_type (id, trip_type)
VALUES
(1, 'Street-hail'),
(2, 'Dispatch');

CREATE TABLE capstone_de.group_3_schema.dim_vendor_id (
    id INT,
    vendor VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_vendor_id (id, vendor)
VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc.');
