# Trip Duration Distribution
SELECT
  ROUND(TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE)) AS trip_duration_min,
  COUNT(*) AS trip_count
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
WHERE 
  tpep_dropoff_datetime > tpep_pickup_datetime
GROUP BY trip_duration_min
ORDER BY trip_duration_min;