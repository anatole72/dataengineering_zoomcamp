Solutions tasks

task 1

```select * from myproject.my_nyc_tripdata.ext_green_taxi```

task 2 

```select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY```

Task 3

Select the option that does NOT apply for materializing fct_taxi_monthly_zone_revenue:

Ans: dbt run --select models/staging/+

Task 4

Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile: If model_type is 'core', it directly uses the environment variable DBT_BIGQUERY_TARGET_DATASET. DBT_BIGQUERY_TARGET_DATASET must be defined, or it will fail. TRUE

Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile: If model_type is NOT 'core', it first tries to use DBT_BIGQUERY_STAGING_DATASET, but if it's not defined, it falls back to DBT_BIGQUERY_TARGET_DATASET. DBT_BIGQUERY_STAGING_DATASET is optional. FALSE

When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET: When model_type == 'core', only DBT_BIGQUERY_TARGET_DATASET is used. TRUE

When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET: If DBT_BIGQUERY_STAGING_DATASET is defined, it will be used; otherwise, DBT_BIGQUERY_TARGET_DATASET is used as the fallback. TRUE

When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET: Same logic as in the previous case. TRUE


task 5

```
{{ config(materialized='table') }}

with quarterly_revenue as (
SELECT
service_type,
EXTRACT(YEAR FROM pickup_datetime) AS year,
EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
SUM(total_amount) AS revenue
FROM {{ ref('fact_trips') }}
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
GROUP BY service_type, year, quarter
),

quarterly_trends AS (
SELECT
curr.year,
curr.quarter,
curr.service_type,
curr.revenue,
prev.revenue AS prev_year_revenue,
(curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0) AS yoy_growth
FROM quarterly_revenue curr
LEFT JOIN quarterly_revenue prev
ON curr.service_type = prev.service_type
AND curr.quarter = prev.quarter
AND curr.year = prev.year + 1
)
SELECT * FROM quarterly_trends```

Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}


Task 6: P97/P95/P90 Taxi Monthly Fare

Create a new model fct_taxi_trips_monthly_fare_p95.sql
Filter out invalid entries (fare_amount > 0, trip_distance > 0, and payment_type_description in ('Cash', 'Credit Card'))
Compute the continous percentile of fare_amount partitioning by service_type, year and and month
Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?

Head over to dbt and create a new file under core models "fct_taxi_trips_monthly_fare_p95.sql"


dbt run --select fct_taxi_trips_monthly_fare_p95



```sql

```SELECT DISTINCT 
    service_type, 
    year, 
    month, 
    p97, 
    p95, 
    p90 
FROM `dbt_andrey.dbt_output.fct_taxi_trips_monthly_fare_p95`
WHERE month = 4 AND year = 2020;```


Answer: green service type: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow service_type: {p97: 31.5, p95: 25.5, p90: 19.0}
