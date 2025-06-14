CREATE OR REPLACE MODEL `gcp-d-461809.central_dataset.fare_prediction_model`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['total_amount']
) AS 
SELECT
  trip_distance,
  passenger_count,
  pickup_hour,
  pickup_borough,
  dropoff_borough,
  total_amount
FROM 
  `gcp-d-461809.central_dataset.yellow_tripdate_ml_ready`