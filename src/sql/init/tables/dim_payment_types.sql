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
