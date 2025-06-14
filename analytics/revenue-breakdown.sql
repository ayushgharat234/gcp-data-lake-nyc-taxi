# Revenue Breakdown by Payment Type
SELECT
  payment_type,
  COUNT(*) AS num_trips,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(SUM(tip_amount), 2) AS ag_tip
FROM `gcp-d-461809.central_dataset.yellow_tripdata_curated`
GROUP BY payment_type
ORDER BY total_revenue DESC;
