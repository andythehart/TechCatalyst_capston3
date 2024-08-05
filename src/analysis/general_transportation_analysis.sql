-- the following queries answer questions in the general transportation analysis block
-- some of these will probably make good views for a reporting table

-- trip duration analysis
SELECT day_of_week, AVG(trip_duration)
FROM fact_green_yellow
GROUP BY day_of_week;

SELECT timeofday, AVG(trip_duration)
FROM fact_green_yellow
GROUP BY timeofday;

SELECT taxi_type, AVG(trip_duration)
FROM fact_green_yellow
GROUP BY taxi_type;

-- Trip distance analysis
SELECT borough, AVG(trip_distance)
FROM fact_green_yellow
INNER JOIN dim_zone_lookup ON fact_green_yellow.pulocationid = dim_zone_lookup.locationid
GROUP BY borough;

SELECT timeofday, AVG(trip_distance)
FROM fact_green_yellow
GROUP BY timeofday;

SELECT taxi_type, AVG(trip_distance)
FROM fact_green_yellow
GROUP BY taxi_type;

-- Fare Analysis
SELECT timeofday, AVG(fare_amount)
FROM fact_green_yellow
GROUP BY timeofday;

SELECT taxi_type, AVG(fare_amount)
FROM fact_green_yellow
GROUP BY taxi_type;

SELECT payment_type, AVG(fare_amount)
FROM fact_green_yellow
GROUP BY payment_type;

-- Traffic Congestion Analysis
SELECT hour, COUNT(*) AS total_rides, AVG(trip_duration)
FROM fact_green_yellow
GROUP BY hour
ORDER BY hour;

SELECT day_of_week, PULOCATIONID, COUNT(*) AS total_rides, AVG(trip_duration)
FROM fact_green_yellow
GROUP BY day_of_week, PULOCATIONID
ORDER BY PULOCATIONID;

-- Passenger Demand
SELECT day_of_week, timeofday, PULOCATIONID, COUNT(*) AS total_rides
FROM fact_green_yellow
GROUP BY day_of_week, timeofday, PULOCATIONID;
