# Vendor Performance Overview
SELECT 
  VendorID,
  COUNT(*) AS total_trips,
  ROUND(SUM(total_amount), 2) AS total_earning,
  ROUND(AVG(total_amount), 2) AS avg_earning_per_trip
FROM
  `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY VendorID
ORDER BY total_earning DESC;