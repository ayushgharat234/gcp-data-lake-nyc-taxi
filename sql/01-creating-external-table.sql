# Creating an external table 
CREATE OR REPLACE EXTERNAL TABLE `gcp-d-461809.central_dataset.yellow_tripdata_ext`
OPTIONS(
  format = 'PARQUET',
  uris = ['gs://nyc-taxi-datalake-project/raw/yellow_tripdata_2025-*.parquet']  # Only considering the regex yellow_trip_2025-
);

# First few rows
SELECT *
FROM `gcp-d-461809.central_dataset.yellow_tripdata_ext`
LIMIT 10;