## Module 2 Homework

### Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` tas__k)?
- **_ _128.3 MB*_*___
- 134.5 MB
- 364.7 MB
- 692.6 MB

#### Answer
the size of the file `yellow_tripdata_2020-12.csv`  is **128.3 MB**. 


2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `**_green_tripdata_2020-04.csv`*_*
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

#### Answer 
 `_**green_tripdata_2020-04.csv`**_.


3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- 13,537.299
- **_24,648,499_**
- 18,324,219
- 29,430,127

#### Answer 
 let us query the dataset `yellow_tripdata` with : 
```sql
SELECT COUNT(*) AS total_rows
FROM `trip_data_all.yellow_tripdata`
WHERE _TABLE_SUFFIX LIKE '2020%';
```
 answer **24,648,499**.

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- _**1,734,051**_
- 1,342,034

#### Answer 
Did the same thing as below here : 
```sql
SELECT COUNT(*) 
FROM `trip_data_all.green_tripdata` 
WHERE _TABLE_SUFFIX LIKE '2020%';
```
 **1,734,051**.

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- _**1,925,152**_
- 2,561,031

#### Answer
run `06_gcp_taxi_scheduled` and run a backfill execution.


Query : 
```sql


SELECT COUNT(*) 
FROM `trip_data_all.yellow_tripdata` 
WHERE _TABLE_SUFFIX = '202103';

```

 answer : **1,925,152**

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

#### Correct Answer:
Add a timezone property set to America/New_York in the Schedule trigger configuration

So, the answer is **`America/New_York`**.
