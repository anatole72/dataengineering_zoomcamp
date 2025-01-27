SELECT 
    COUNT(CASE WHEN trip_distance <= 1.0 THEN 1 END) AS from_0_to_1_mile,
    COUNT(CASE WHEN trip_distance > 1.0 AND trip_distance <= 3.0 THEN 1 END) AS from_1_to_3_miles,
    COUNT(CASE WHEN trip_distance > 3.0 AND trip_distance <= 7.0 THEN 1 END) AS from_3_to_7_miles,
    COUNT(CASE WHEN trip_distance > 7.0 AND trip_distance <= 10.0 THEN 1 END) AS from_7_to_10_miles,
    COUNT(CASE WHEN trip_distance > 10.0 THEN 1 END) AS from_10_to_inf_miles
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
  AND lpep_dropoff_datetime < '2019-11-01';