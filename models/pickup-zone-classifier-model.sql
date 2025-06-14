CREATE OR REPLACE MODEL `gcp-d-461809.central_dataset.pickup_zone_classifier`
OPTIONS(
  model_type = 'logistic_reg',
  input_label_cols = ['pickup_service_zone']
) AS 
SELECT
  trip_distance,
  passenger_count,
  pickup_hour,
  dropoff_borough,
  pickup_service_zone
FROM
  `gcp-d-461809.central_dataset.yellow_tripdate_ml_ready`
WHERE
  pickup_service_zone IS NOT NULL;