CREATE OR REPLACE VIEW capstone_de.group_3_schema.view_outliers_by_location (pulocationid, num_outliers) AS (
    SELECT pulocationid, COUNT(*)
    FROM outliers_report
    GROUP BY pulocationid
    ORDER BY pulocationid
);
