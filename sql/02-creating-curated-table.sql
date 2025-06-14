# Creating a curated table
CREATE OR REPLACE TABLE `gcp-d-461809.central_dataset.yellow_tripdata_curated`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID, DOLocationID AS 
SELECT 
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  Airport_fee,
  cbd_congestion_fee
FROM `gcp-d-461809.central_dataset.yellow_tripdata_ext`
WHERE 
  trip_distance > 0
  AND passenger_count BETWEEN 1 AND 6
  AND fare_amount > 0
  AND total_amount < 500
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND EXTRACT(YEAR FROM tpep_pickup_datetime) = 2025
  AND EXTRACT(YEAR FROM tpep_dropoff_datetime) = 2025;