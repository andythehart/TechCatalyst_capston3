USE DATABASE capstone_de;
USE SCHEMA group_3_schema;
-- Dim tables
CREATE TABLE dim_ratecode (
    id INT,
    ratecode VARCHAR
);
INSERT INTO dim_ratecode (id, ratecode)
VALUES
(1, 'Standard Rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated Fare'),
(6, 'Group Ride');

CREATE TABLE dim_payment_type (
    id INT,
    payment_type VARCHAR
);
INSERT INTO dim_payment_type (id, payment_type)
VALUES
(1, 'Credit Card'),
(2, 'Cash'),
(3, 'No Charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided Trip');

CREATE TABLE dim_store_fwd_flag (
    y_n VARCHAR,
    s VARCHAR
);
INSERT INTO dim_store_fwd_flag (y_n, s)
VALUES
('Y', 'store and forward trip'),
('N', 'not a store and forward trip');

CREATE TABLE dim_trip_type (
    id INT,
    trip_type VARCHAR
);
INSERT INTO dim_trip_type (id, trip_type)
VALUES
(1, 'Street-hail'),
(2, 'Dispatch');

CREATE TABLE dim_vendor_id (
    id INT,
    vendor VARCHAR
);
INSERT INTO dim_vendor_id (id, vendor)
VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc.');
