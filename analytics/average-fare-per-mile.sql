# Average fare per mile per hour
SELECT
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
  AVG(fare_amount/NULLIF(trip_distance, 0)) AS abg_fare_per_mile
FROM
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY hour
ORDER BY hour;