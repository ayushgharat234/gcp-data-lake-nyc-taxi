# Average trip distance covered per hour per day
SELECT
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, 
  AVG(trip_distance) AS avg_distance
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY hour
ORDER BY hour;
