WITH biggest_pickup_loc_ids AS (
    SELECT "pulocationid"
    FROM green_taxi_trips
    WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
    GROUP BY "pulocationid"
    HAVING SUM(total_amount) > 13000.0
)
SELECT "zone"
FROM taxi_zone_lookup
WHERE "locationid" IN (SELECT * FROM biggest_pickup_loc_ids);