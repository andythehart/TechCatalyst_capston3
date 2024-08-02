# Initial Data Report

Let's investigate the quality of the data we have.

## Nonsensical Records

What are conditions that don't make sense?

1. A `trip_distance` of `0`.
```sql
SELECT COUNT(*)
FROM fact_green_yellow
WHERE trip_distance = 0;
```

There are `735962` records with a 0 distance, we'll have to remove those.
Let's check if there are any wildly high trip distances.
```sql
SELECT AVG(trip_distance) AS average, MEDIAN(trip_distance) AS median,
    PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY trip_distance) as Q1,
    PERCENTILE_CONT(.5) WITHIN GROUP (ORDER BY trip_distance) as Q2,
    PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY trip_distance) as Q3,
    PERCENTILE_CONT(.80) WITHIN GROUP (ORDER BY trip_distance) as Q80,
    PERCENTILE_CONT(.95) WITHIN GROUP (ORDER BY trip_distance) as Q95,
    PERCENTILE_CONT(.99) WITHIN GROUP (ORDER BY trip_distance) as Q99,
    PERCENTILE_CONT(.995) WITHIN GROUP (ORDER BY trip_distance) as Q995,
    PERCENTILE_CONT(.999) WITHIN GROUP (ORDER BY trip_distance) as Q999,
    PERCENTILE_CONT(.9999) WITHIN GROUP (ORDER BY trip_distance) as Q9999,
    PERCENTILE_CONT(.99999) WITHIN GROUP (ORDER BY trip_distance) as Q99999,
FROM fact_green_yellow;
```

| AVERAGE | MEDIAN | Q1	| Q2 | Q3 | Q80 | Q95 | Q99 | Q995 | Q999 | Q9999 | Q99999 |
| ------- | ------ | -- | -- | -- | --- | --- | --- | ---- | ---- | ----- | ------ |
| 4.56  |	1.73   | 1  |1.73|3.3|4.08|13.97  |	20| 21.69 | 29.6 | 58.3	  | 43953.7 |
Clearly, something goes wrong at the upper end.

Using the 9999th percentile
```sql
SELECT COUNT(*)
FROM fact_green_yellow
WHERE trip_distance > 60;
```
There are `2799` records that exceed this travel distance.

2. Impossibly high trip fares.
To get an idea of this, we can compute percentiles.
```sql
SELECT AVG(fare_amount) AS average, MEDIAN(fare_amount) AS median,
    PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY fare_amount) as Q1,
    PERCENTILE_CONT(.5) WITHIN GROUP (ORDER BY fare_amount) as Q2,
    PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY fare_amount) as Q3,
    PERCENTILE_CONT(.80) WITHIN GROUP (ORDER BY fare_amount) as Q80,
    PERCENTILE_CONT(.95) WITHIN GROUP (ORDER BY fare_amount) as Q95,
    PERCENTILE_CONT(.99) WITHIN GROUP (ORDER BY fare_amount) as Q99,
    PERCENTILE_CONT(.995) WITHIN GROUP (ORDER BY fare_amount) as Q995,
    PERCENTILE_CONT(.999) WITHIN GROUP (ORDER BY fare_amount) as Q999,
    PERCENTILE_CONT(.9999) WITHIN GROUP (ORDER BY fare_amount) as Q9999,
    PERCENTILE_CONT(.99999) WITHIN GROUP (ORDER BY fare_amount) as Q99999,
FROM fact_green_yellow;
```
| AVERAGE | MEDIAN | Q1	| Q2 | Q3 | Q80 | Q95 | Q99 | Q995 | Q999 | Q9999 | Q99999 |
| ------- | ------ | -- | -- | -- | --- | --- | --- | ---- | ---- | ----- | ------ |
| 19.288  |	13.5   | 9.3|13.5|21.9|25.55|65   |	77.9|91.29 |147.9 |300	  | 558.63 |

`$300` seems to be a good cutoff.
```sql
SELECT COUNT(*)
FROM fact_green_yellow
WHERE fare_amount > 300;
```

There are only `2793` records with a `fare_amount` > 300

3. Passenger Counts
A passenger count of 0 doesn't make sense.

```sql
SELECT DISTINCT passenger_count as pass_cnt, COUNT(*) as total_records
FROM fact_green_yellow
GROUP BY pass_cnt
ORDER BY pass_cnt;
```
| pass_cnt | total_records |
| -------- | ------------- |
| 0        |	359863     |
| 1        |	21620064   |
| 2        |	4157107    |
| 3        |	964983     |
| 4        |	568541     |
| 5        |	333181     |
| 6        |	214725     |
| 7        |	89         |
| 8        |	244        |
| 9        |	66         |
| null     |    2204270    |

It will likely be best to assume that null or 0 means just 1 passenger.

## When?
Let's investigate the time range of the data.
```sql
SELECT DISTINCT YEAR(pickup_datetime) AS "year", COUNT(*) AS totals_by_year
FROM fact_green_yellow
GROUP BY "year";
```

| year | totals_by_year |
| ---- | -------------- |
| 2002 | 12             |
| 2008 | 10             |
| 2009 | 18             |
| 2023 | 13345165       |
| 2024 | 17077928       |

Clearly, we can get rid of the records for the years 2002, 2008 and 2009.
