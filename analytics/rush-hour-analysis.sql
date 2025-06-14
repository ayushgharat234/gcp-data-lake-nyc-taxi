#  Rush Hour Analysis: Trip Volume vs. Earnings
SELECT 
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
  COUNT(*) AS trips,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(trip_distance), 2) AS avg_distance
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY hour
ORDER BY hour;