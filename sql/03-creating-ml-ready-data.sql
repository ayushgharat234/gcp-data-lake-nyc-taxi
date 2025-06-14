CREATE OR REPLACE TABLE `gcp-d-461809.central_dataset.yellow_tripdate_ml_ready` AS
SELECT
  SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
  SAFE_CAST(passenger_count AS INT64) AS passenger_count, -- Removed trailing dot
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
  DATETIME_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) AS trip_duration_mins,
  pickup_zone.Borough AS pickup_borough, -- Removed trailing dot
  dropoff_zone.Borough AS dropoff_borough,
  pickup_zone.service_zone AS pickup_service_zone,
  dropoff_zone.service_zone AS dropoff_service_zone,
  total_amount,
  CASE
    WHEN trip_distance = 0 AND DATETIME_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) <= 60 THEN TRUE
    ELSE FALSE
  END AS is_cancellation -- Added missing comma (implied by next line being FROM)
FROM
  `gcp-d-461809.central_dataset.yellow_tripdata_curated` AS trips
LEFT JOIN
  `gcp-d-461809.lookup_dataset.taxi_zones` AS pickup_zone
ON
  trips.PULocationID = pickup_zone.LocationID
LEFT JOIN
  `gcp-d-461809.lookup_dataset.taxi_zones` AS dropoff_zone
ON
  trips.DOLocationID = dropoff_zone.LocationID
WHERE
  trip_distance >= 0
  AND total_amount > 0
  AND total_amount < 300
  AND DATETIME_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) BETWEEN 1 AND 120
  AND pickup_zone.Borough IS NOT NULL
  AND dropoff_zone.Borough IS NOT NULL;