-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `eighth-surfer-375717.fhv_taxi.external_fhv_trip`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect_de_zoom/data/fhv/fhv_tripdata_2019-*.csv']
);


Q1: What is the count for fhv vehicle records for year 2019?

SELECT count(*) FROM `eighth-surfer-375717.fhv_taxi.external_fhv_trip`;



Q2: Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.  
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

--Count from external table
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `eighth-surfer-375717.fhv_taxi.external_fhv_trip`;

--Count from native table
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `eighth-surfer-375717.fhv_taxi.fhv_trip`;


Q3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

SELECT count(*) FROM `eighth-surfer-375717.fhv_taxi.external_fhv_trip`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;


Q4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Partition by pickup_datetime Cluster on affiliated_base_number

Q5: Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).  
Use the BQ table you created earlier in your from clause and note the estimated bytes. -- -- 
Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE eighth-surfer-375717.fhv_taxi.partitioned_fhv_trip
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM eighth-surfer-375717.fhv_taxi.external_fhv_trip;


--Count affiliated_base_number from non_partitioned data
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `eighth-surfer-375717.fhv_taxi.fhv_trip`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';


--Count affiliated_base_number from partitioned data
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `eighth-surfer-375717.fhv_taxi.partitioned_fhv_trip`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';



Q6: Where is the data stored in the External Table you created?

GCS

Q7: It is best practice in Big Query to always cluster your data:

Depends on the data.

The statement is true if the data is big.
