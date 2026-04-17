cd into folder `task_2` and run `docker-compose up --build`

then open terminal of master container and run `python /notebooks/main.py`


List of urls to get the training datasets:

Pipeline A — E-commerce Behaviour Data
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Pipeline B — NYC Taxi Trip Records
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Pipeline B — Taxi Zone Lookup Table
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv


Download it to the folder `/task_2/data/`


# Summary

For this task I worked with two datasets:
a 42-million-row e-commerce dataset and a two full years of NYC taxi trip records. 
Across two pipelines I applied four optimisation techniques
— broadcast joins, partition tuning, caching, and Adaptive Query Execution — 
and measured the runtime difference between unoptimised and optimised versions 
using Spark's noop writer to ensure full pipeline execution.

### Caching
Caching turned out to be the single biggest win in Pipeline A. 
The unoptimised version was reading the same 5.5GB CSV file multiple times
— once for purchases, once for views, once for brand lookups and so on. 
Each of those was a full disk scan. By caching the DataFrame after the first 
read, subsequent operations pulled data from memory instead of disk, 
which significantly reduced the total pipeline runtime.

### Broadcast joins
Broadcast joins were the dominant technique in pipelines where the 
data was smaller or already cached. When Spark joins two DataFrames by default, 
it shuffles both sides across the network. For small lookup tables like brand 
tiers (3,446 rows) or taxi zones (265 rows), this shuffle is completely 
unnecessary. Broadcasting sends the small table to every executor once, 
and each partition of the large table joins against it locally. 
This eliminates the shuffle entirely and switches Spark from a slow 
SortMergeJoin to a fast BroadcastHashJoin.

### Partition tuning
Partition tuning is about right-sizing the number of tasks Spark creates 
after a shuffle. The default is 200 partitions regardless of data size. 
For a dataset that fits into a few hundred megabytes after filtering, 
200 partitions means hundreds of near-empty tasks being scheduled and 
managed for no reason. Reducing this to 8 — aligned to the number of 
available worker cores — cut scheduling overhead noticeably.

### Adaptive Query Execution (AQE)
Adaptive Query Execution (AQE) works alongside the other techniques 
rather than replacing them. It lets Spark look at actual data statistics 
at runtime and adjust its plan on the fly — merging small partitions, 
splitting skewed ones, and choosing better join strategies than it could 
predict upfront. It acts as a safety net that catches imbalances that static 
configuration misses.

---

One thing worth noting is that these techniques don't always stack neatly.
Caching has an upfront cost, you pay to read and store the data before 
the pipeline even starts. On small datasets like the Parquet taxi files, 
that cost outweighed the benefit because Parquet reads are already fast and 
caching only slowed overall execution time.
This was a useful real-world lesson: optimization requires understanding 
your data, not just applying techniques mechanically.