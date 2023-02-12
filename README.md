# Data-Warehouse-Data-Engineering-Zoomcamp-Week-3



# Data Warehouse & Big Query

This week we learned more about Data Warehouse & Big Query.

## What is Data Warehouse?
- Data Warehouse is a central repository to store data from various sources.
- Data might be transformed before getting loaded into a database (ETL).
- In some cases, data might not be transformed but it maintains inside the Data Warehouse (in a staging area). After data get transformed (ELT), which also happens inside the  Data Warehouse, then it will be loaded into a database.
 
## What is Big Query?
- BigQuery (BQ) is a serverless Data Warehouse, managed by Google.  
- BQ supports all data types and works across cloud. 
- And BQ supports SQL-like query, which we will be working on today



## Scope of Work

We will put knowledge from previous weeks into action.  

Tasks of week 3 are:
  1. Collect data from sources
  2. Store data in google cloud
  3. Work with data in Big Query (Create an external table & native table, Compared partitioned data vs non-partitioned data and observe its performance)
  4. Query data with SQL

And this week we are free to use any tools/method in our favorites. 

Since I've been working with the Prefect Orchestration tool in the past weeks. I chose to continue working with Prefect because of its simple yet effective. 

## Data Collection & Store in Google Cloud Storage (GCS)

By using Prefect, we can create a workflow to complete 2 tasks in one action, which are:
 1. Data collection
 2. Upload data into Google Cloud


Steps:
 1. Create a python file, I named it "data_to_gcs.py" which contains functions as follow:

``` 
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df
    

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv file"""
    path = Path(f"data/fhv/{dataset_file}.csv")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local csv file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    return


@flow()
def web_to_gcs(year: int, month: int) -> None:
    """The main function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2019
):
    for month in months:
        web_to_gcs(year, month)

if __name__ == "__main__":
    months = list(range(1,13))
    year = 2019
    etl_parent_flow(months, year)

``` 


 2. Run data_to_gcs.py 
 3. Now all data are stored in local & on GCS



## Google Big Query

After having data stored in GCS.

You can now work with data using Big Query. 

Steps:
 1. Create dataset 
 2. Create table (2 options: **Native table and External table**)
 3. Load data into the table

### What are the different between those two types of table?

#### External Table in Big Query
- Instead of create a table and store all the data in that table inside the Big Query, we create an external table.
- External table is a table definition (which you will use to query data). 
  But all the data will be stored outside Big Query. In this case, it's stored in GCS.
- Even our data is not in the Big Query but we can still query them using all the tool e.g. SQL. This concept is called "federated query".

#### Native Table
- Native table is a table that stores all data inside Big Query
- More expensive than using external table 
        

### Step 1: Create Dataset

- Check your region that you've set on your Bucket
  On Terminal, run

```
gsutil ls -Lb gs://prefect_de_zoom
```

Back to your gcloud,

Go to >> Big Query
 - Click ⋮ (three dot next to your project name)
	- Setup dataset name & region
	- Click Create Dataset

### Step 2: Create Table

 #### How to Create an External Table
 On your [gcloud/bigquery](https://console.cloud.google.com/bigquery)
	- Click + sign (which is "compose new query")
	- Run the query below to creat an **external table** 

```
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<project name>.<dataset name>.<table name>`
OPTIONS (
  format = 'CSV',
  uris = ['gs://<bucket name>/path/to/csv file/<file name>.csv']
);
```

#### How to Create a Native Table & Load Data into Table
 On your [gcloud/bigquery](https://console.cloud.google.com/bigquery)
 - Click +ADD DATA
 - Select Google Cloud Storage as Source
   - Browse file that you want to work on or write path to file as follow, to gather multiple files into a table
     ```<your bucket name>/path/to/file/fhv_tripdata_2019-*.csv``` 
   - Define dataset name, table name
   - Select table type: Native Table
   - Create table
 
 Now you have your table & can proceed to answer the questions in homework!


## SQL Query

One you have your dataset & table.

You can now using SQl tool to query data.


Refering to Homework Week 3

Here are links to some useful query:

[Big Query SQL](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query.sql)


[Big Query Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_hw.sql)



==========Thank You==========
