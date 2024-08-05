create or replace view fastest_route_distance as
  SELECT
  fgy.pickup_datetime,
  fgy.pulocationid,
  p_zone.zone AS pickup_zone,
  DROPOFF_DATETIME,
  fgy.dolocationid,
  d_zone.zone AS dropoff_zone,
  fgy.hour,
  fgy.TRIP_DURATION,
  fgy.Trip_distance,
  --fgy.total_amount
  
  
FROM
  fact_green_yellow AS fgy
  JOIN dim_zone_lookup AS p_zone ON fgy.pulocationid = p_zone.locationid
  JOIN dim_zone_lookup AS d_zone ON fgy.dolocationid = d_zone.locationid
  --where pulocationid = 164 and dolocationid = 42
  where trip_duration >= 1;