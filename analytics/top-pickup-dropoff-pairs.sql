# Top Pickup-Dropoff Pairs by Trip Count
SELECT 
  PULocationID,
  DOLocationID,
  COUNT(*) AS trip_count
FROM `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY PULocationID, DOLocationID
ORDER BY trip_count DESC
LIMIT 10;