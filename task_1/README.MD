## Objective:

Gain hands-on PySpark experience by building and running 
at least 3 production-ready ETL pipelines on Databricks 
using the Medallion Architecture (Bronze - Silver - Gold), 
covering ingestion, transformation, and Delta Lake upserts (MERGE/CDC).

## Expected result:

At least 3 working PySpark pipelines deployed on Databricks; peer-reviewed code with no critical issues; demonstrable knowledge of Delta Lake ACID transactions and time travel.

----
In task_1 folder you can find source .py files for Databricks notebooks.

### First pipeline

For the first pipeline I used sample data from Databricks
`samples.bakehouse`

I uploaded `samples.bakehouse.*` tables as JSON files to workspace
volume (*/Volumes/workspace/lnd/raw_data*) to imitate source system.


### Second pipeline

For the second pipeline I used ecommerce data from 
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data

`2019-Oct.csv.gz` in particular.

I uploaded it to the */Volumes/workspace/lnd/raw_data/ecommerce_store/*

### Third pipeline

Two previous pipelines were developed with classic Pyspark as Databricks
notebooks.

For the third pipeline, I tried to use Lakeflow Spark Declarative Pipelines
native only to Databricks. This framework enables smooth Change Data Capture and 
schema evolution. The framework promises faster development time and fewer
lines of code. Unfortunately, I wasn't able to make it work.

For this pipeline I used NYC taxi data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Direct download link: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
(change the year-month suffix for each file)

What is interesting is that data is in parquet files.
