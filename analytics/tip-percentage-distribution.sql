# Tip Percentage Distribution
SELECT
  ROUND((tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS tip_percent,
  COUNT(*) AS trip_count
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
WHERE 
  tip_amount > 0
GROUP BY tip_percent
ORDER BY tip_percent DESC
LIMIT 20;
