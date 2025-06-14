# Daily Average Fare Per Mile
SELECT
  DATE(tpep_pickup_datetime) AS trip_date,
  ROUND(AVG(fare_amount/NULLIF(trip_distance, 0)), 2) AS avg_fare_per_mile
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY trip_date
ORDER BY trip_date;
