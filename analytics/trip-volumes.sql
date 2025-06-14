# Trip Volume by Day
SELECT
  DATE(tpep_pickup_datetime) AS trip_date,
  COUNT(*) AS total_trips
FROM
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY trip_date
ORDER BY trip_date;