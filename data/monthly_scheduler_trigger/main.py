import datetime
import json
from google.cloud import pubsub_v1

PROJECT_ID = "gcp-d-461809"
TOPIC_ID = "trigger-nyc-pipeline"

def monthly_trigger(request):
    now = datetime.datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")

    message = json.dumps({"year": year, "month": month})
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    publisher.publish(topic_path, data=message.encode("utf-8"))

    return f"Published message for {year}-{month}", 200