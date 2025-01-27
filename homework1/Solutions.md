# Module 1 Homework: Docker & SQL

Shell commands or SQL queries necessary for answering the questions.


## Task 1. Understanding docker first run

Commands:

```bash
docker run --rm -it --entrypoint=bash python:3.12.8
# inside the container:
pip --version
```

Output:

```plaintext
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

## Task 2

Hostname and Port for pgAdmin to Connect to PostgreSQL:
Hostname: db (the service name of the PostgreSQL container).

``` services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

Solution

Since both services are running in the same Docker Compose network, pgadmin should use db:5432 to connect to the postgres database.
```plaintext
Host: db
Port: 5432
```


## Task 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles

Solution

Query:
```sql
SELECT 
    COUNT(CASE WHEN trip_distance <= 1.0 THEN 1 END) AS from_0_to_1_mile,
    COUNT(CASE WHEN trip_distance > 1.0 AND trip_distance <= 3.0 THEN 1 END) AS from_1_to_3_miles,
    COUNT(CASE WHEN trip_distance > 3.0 AND trip_distance <= 7.0 THEN 1 END) AS from_3_to_7_miles,
    COUNT(CASE WHEN trip_distance > 7.0 AND trip_distance <= 10.0 THEN 1 END) AS from_7_to_10_miles,
    COUNT(CASE WHEN trip_distance > 10.0 THEN 1 END) AS from_10_to_inf_miles
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
  AND lpep_dropoff_datetime < '2019-11-01';
```

Data output:

| from_0_to_1_mile | from_1_to_3_miles | from_3_to_7_miles | from_7_to_10_miles | from_10_to_inf_miles |
|------------------|-------------------|-------------------|--------------------|----------------------|
| 104802           | 198924            | 109603            | 27678              | 35189                |




## Task 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

2019-10-11
2019-10-24
2019-10-26
2019-10-31

Solution

Query:

```sql
SELECT DATE(lpep_pickup_datetime)
FROM green_taxi_trips
WHERE trip_distance = (
    SELECT MAX(trip_distance)
    FROM green_taxi_trips
    );
```

Data output:

| date |
|------|
| 2019-10-31 |


## Task 5. Three biggest pickup zones


Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.

East Harlem North, East Harlem South, Morningside Heights
East Harlem North, Morningside Heights
Morningside Heights, Astoria Park, East Harlem South
Bedford, East Harlem North, Astoria Park

Solution

Query:

```sql
WITH pickups AS (
    SELECT "pulocationid"
    FROM green_taxi_trips
    WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
    GROUP BY "pulocationid"
    HAVING SUM(total_amount) > 13000.0
)
SELECT "zone"
FROM taxi_zone_lookup
WHERE "locationid" IN (SELECT * FROM pickups);
```

Data output:

| Zone |
|------|
| East Harlem North |
| East Harlem South |
| Morningside Heights |


## Task 6   Largest tip

For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip

We need the name of the zone, not the ID.

Yorkville West
JFK Airport
East Harlem North
East Harlem South


Solution

Query:

```sql
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
```

Data output:

| Zone |
|------|
| JFK Airport |

## Task 7.  Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:

Downloading the provider plugins and setting up backend,
Generating proposed changes and auto-executing the plan
Remove all resources managed by terraform`

Solution


terraform init
terraform plan && terraform apply -auto-approve
terraform destroy