import json
import logging

from google.cloud import storage, pubsub_v1


def upload_to_storage(local_file_path, bucket_name, destination_blob_name):
    # 將檔案上傳至Storage
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    

def publish_to_pubsub(stock_dt,project_id, topic_name, error_message):
    # 發送錯誤訊息至pubsub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    message = {
        "error_stock_dt":stock_dt,
        "error_message": error_message
    }
    message_str = json.dumps(message)
    future = publisher.publish(topic_path, data=message_str.encode("utf-8"))
    result = future.result()