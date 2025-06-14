CREATE VIEW `gcp-d-461809.central_dataset.predicted_duration_view` AS
SELECT
  *
FROM
  ML.PREDICT(MODEL `gcp-d-461809.central_dataset.duration_prediction_model`,
    (SELECT
      trip_distance,
      passenger_count,
      pickup_hour,
      pickup_borough,
      dropoff_borough
     FROM
      `gcp-d-461809.central_dataset.yellow_tripdate_ml_ready`)
  )