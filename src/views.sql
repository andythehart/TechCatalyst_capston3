-- Views for analysis
CREATE OR REPLACE VIEW capstone_de.group_3_schema.view_outliers_by_location (pulocationid, num_outliers) AS (
    SELECT pulocationid, COUNT(*)
    FROM outliers_report
    GROUP BY pulocationid
    ORDER BY pulocationid
);

CREATE OR REPLACE VIEW capstone_de.group_3_schema.view_potential_fraud_dates (day_of_month, month, year, total_fare_amount) AS (
    SELECT day_of_month, month, year, SUM(fare_amount)
    FROM outliers_report
    GROUP BY day_of_month, month, year
    ORDER BY year, month ASC, day_of_month
);
