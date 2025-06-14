# Heatmap Input: Location-to-Location Revenue Matrix
SELECT
  PULocationID,
  DOLocationID,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  COUNT(*) AS trip_count
FROM
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY PULocationID, DOLocationID
HAVING trip_count > 50
ORDER BY total_revenue DESC
LIMIT 100;