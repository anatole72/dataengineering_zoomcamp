SELECT
    zn_do."zone"
FROM
    green_taxi_trips AS tr
    INNER JOIN taxi_zone_lookup AS zn_pu
        ON tr."pulocationid" = zn_pu."locationid"
    INNER JOIN taxi_zone_lookup AS zn_do
        ON tr."dolocationid" = zn_do."locationid"
WHERE tr.lpep_pickup_datetime >= '2019-10-01'
  AND tr.lpep_pickup_datetime < '2019-11-01'
  AND zn_pu."zone" = 'East Harlem North'
ORDER BY tr.tip_amount DESC
LIMIT 1;