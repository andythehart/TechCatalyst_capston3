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
