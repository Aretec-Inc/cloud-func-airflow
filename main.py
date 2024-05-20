from cloudevents.http import CloudEvent
import functions_framework


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def uploader(cloud_event: CloudEvent) -> tuple:
    """This function is triggered by a change in a storage bucket.

    Args:
        cloud_event: The CloudEvent that triggered this function.
    Returns:
        The event ID, event type, bucket, name, metageneration, and timeCreated.
    """
    from google.cloud import storage
    import json
    import uuid
    import requests
    from requests.auth import HTTPBasicAuth
    import os

    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    
    AIRFLOW_WEBSERVER = os.environ['AIRFLOW_WEBSERVER']
    AIRFLOW_WEBSERVER = f"{AIRFLOW_WEBSERVER}/api/v1/dags/uploader_dag/dagRuns"
    AIRFLOW_USER= os.environ['AIRFLOW_USER']
    AIRFLOW_PASSWORD= os.environ['AIRFLOW_PASSWORD']
    valid_extensions = ['.pdf','.PDF', '.ppt', 'pptx', '.doc', '.docx', '.txt', '.pptx']
    if any(name.endswith(ext) for ext in valid_extensions):
        storage_client = storage.Client()
        file_url = f'gs://{bucket}/{name}'
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(name)
        blob.reload()
        # Fetch and print metadata
        metadata = blob.metadata
        if (not metadata) or metadata =={}:
            return event_id, event_type, bucket, name, metageneration, timeCreated, updated
        domain = metadata['domain']
        domain = domain.split(',')
        domain = json.dumps(domain)
        file_id = metadata['file_id']
        index = metadata['index']
        user_id = metadata['user_id']
        referer = metadata['referer']
        file_name = metadata['name']
        log_index = 'disearch_dev'
        logs_url = os.environ['LOGS_URL']
        
        data = {
        "conf": {
            "fileUrl": file_url,
            "index": index,
            "parser_type": 'parsr',
            "domain": domain,
            "model_type": "vitgpt",
            "file_id": file_id,
            "fileName":file_name,
            "user_id": user_id,
            "referer": referer,
            "log_index": log_index,
            "logs_url": logs_url,
            "transactionId": (str(uuid.uuid4()))
        }
        }
        auth = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)
        config = {
        "url": AIRFLOW_WEBSERVER,
        "json": data,
        "auth": auth
        }
        response = requests.post(**config)
        response.raise_for_status()
        print(f"Event ID: {event_id}")
        print(f"Event type: {event_type}")
        print(f"Bucket: {bucket}")
        print(f"File: {name}")
        print(f"Metageneration: {metageneration}")
        print(f"Created: {timeCreated}")
        print(f"Updated: {updated}")

    return event_id, event_type, bucket, name, metageneration, timeCreated, updated
