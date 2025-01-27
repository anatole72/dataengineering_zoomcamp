SELECT DATE(lpep_pickup_datetime)
FROM green_taxi_trips
WHERE trip_distance = (
    SELECT MAX(trip_distance)
    FROM green_taxi_trips
    );