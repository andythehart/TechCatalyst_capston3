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


-- Views for Collision & Rides Dashboard (Crash Data)

-- Avg Stats View
CREATE OR REPLACE VIEW CAPSTONE_DE.GROUP_3_SCHEMA.AVG_TAXI_STATS_VIEW(
    BOROUGH,
    AVG_PASSENGER_COUNT,
    AVG_TRIP_DISTANCE,
    AVG_RIDE_COUNT,
    AVG_FARE_AMOUNT,
    AVG_TIP_AMOUNT,
    AVG_TOTAL_AMOUNT
)AS 
SELECT  BOROUGH,
        ROUND(AVG(PASSENGER_COUNT), 1) AS AVG_PASSENGER_COUNT,
        ROUND(AVG(TRIP_DISTANCE), 2) AS AVG_TRIP_DISTANCE,
        ROUND(SUM(vendorid), 2) AS AVG_RIDE_COUNT,
        ROUND(AVG(FARE_AMOUNT), 2) AS AVG_FARE_AMOUNT,
        ROUND(AVG(TIP_AMOUNT), 2) AS AVG_TIP_AMOUNT,
        ROUND(AVG(TOTAL_AMOUNT), 2) AS AVG_TOTAL_AMOUNT
FROM FACT_GREEN_YELLOW F
JOIN DIM_ZONE_LOOKUP Z ON F.PULOCATIONID = Z.LOCATIONID
WHERE BOROUGH IN ('Bronx', 'Brooklyn', 'Queens', 'Staten Island', 'Manhattan')
GROUP BY BOROUGH
ORDER BY AVG_RIDE_COUNT;


-- Price by Hour View
CREATE OR REPLACE VIEW CAPSTONE_DE.GROUP_3_SCHEMA.PRICE_HR_VIEW(
    HOUR_OF_DAY,
    TIME_OF_DAY,
    DAY_OF_WEEK,
    RIDE_COUNT,
    BOROUGH
)AS 
SELECT HOUR, TIMEOFDAY, DAY_OF_WEEK, SUM(VENDORID) AS RIDE_COUNT, BOROUGH
FROM FACT_GREEN_YELLOW F
JOIN DIM_ZONE_LOOKUP Z ON F.PULOCATIONID = Z.LOCATIONID
WHERE BOROUGH IN ('Bronx', 'Brooklyn', 'Queens', 'Staten Island', 'Manhattan')
GROUP BY HOUR, TIMEOFDAY, DAY_OF_WEEK, BOROUGH
ORDER BY RIDE_COUNT DESC;


-- Borough Hour Pricing View
CREATE OR REPLACE VIEW CAPSTONE_DE.GROUP_3_SCHEMA.BOROUGH_HR_PRICING_VIEW(
    BOROUGH,
    HOUR,
    AVG_COLLISION_COUNT,
    PERCENT_INCREASE
)AS 
SELECT *
FROM BOROUGH_HR_PRICING;