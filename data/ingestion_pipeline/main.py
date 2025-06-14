import base64
import json
import requests
from flask import Flask, request
from google.cloud import storage

# Initialize the Flask application
app = Flask(__name__)

# Initialize the Google Cloud Storage client.
# This client will be used to interact with Google Cloud Storage buckets and objects.
storage_client = storage.Client()

# Define the name of the Google Cloud Storage bucket where data will be stored.
BUCKET_NAME = "nyc-taxi-datalake-project"

@app.route("/", methods=["POST"])
def pubsub_trigger():
    """
    This function is triggered by a Google Cloud Pub/Sub message.
    It expects a POST request with a JSON payload containing the Pub/Sub message.
    The function downloads a Parquet file from a specified URL and uploads it to Google Cloud Storage.
    """
    # Get the JSON payload from the incoming request.
    data = request.get_json()

    # Check if the "message" key exists in the Pub/Sub payload.
    # This is a standard check for Pub/Sub push subscriptions.
    if "message" not in data:
        print("Invalid Pub/Sub format: 'message' key missing.")
        return "Invalid Pub/Sub format", 400

    # Decode the base64-encoded Pub/Sub message data.
    # Pub/Sub message data is always base64 encoded.
    pubsub_message = base64.b64decode(data["message"]["data"]).decode("utf-8")
    
    # Parse the decoded Pub/Sub message string as a JSON object.
    # This JSON is expected to contain parameters like 'year' and 'month'.
    params = json.loads(pubsub_message)

    # Extract 'year' and 'month' from the parsed parameters.
    # Provide default values ('2025', '01') if they are not present in the message.
    year = params.get("year", "2025")
    month = params.get("month", "01")
    
    # Construct the file name for the Parquet file.
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    
    # Construct the full URL from which the Parquet file will be downloaded.
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    print(f"Attempting to download from: {url}")
    
    # Send an HTTP GET request to download the file from the constructed URL.
    response = requests.get(url)
    
    # Check if the download was successful (HTTP status code 200).
    if response.status_code != 200:
        print(f"Failed to fetch data from: {url}. Status code: {response.status_code}")
        return f"Failed to fetch: {url} (Status: {response.status_code})", 500

    # Define the full path where the file will be stored in the GCS bucket.
    # This path includes a 'raw' prefix for organization within the datalake.
    blob_path = f"nyc-taxi-datalake-project/raw/{file_name}"
    
    # Get a reference to the specified GCS bucket.
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Create a new blob (object) in the bucket with the defined blob_path.
    blob = bucket.blob(blob_path)
    
    # Upload the content of the downloaded file (as bytes) to the GCS blob.
    blob.upload_from_string(response.content)

    print(f"Successfully uploaded to GCS: {blob_path}")
    
    # Return a success message and a 200 OK status code.
    return f"Done: {file_name}", 200