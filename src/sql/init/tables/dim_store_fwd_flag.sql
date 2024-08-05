CREATE TABLE capstone_de.group_3_schema.dim_store_fwd_flag (
    y_n VARCHAR,
    s VARCHAR
);
INSERT INTO capstone_de.group_3_schema.dim_store_fwd_flag (y_n, s)
VALUES
('Y', 'store and forward trip'),
('N', 'not a store and forward trip');
