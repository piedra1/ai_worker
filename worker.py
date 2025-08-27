import pika
import json
import requests
import os
from minio import Minio
from process_video import anonymize_video

# === Base directory (project root) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# === 1. MinIO client config ===
minio_client = Minio(
    "localhost:9002",
    access_key="admin",
    secret_key="password",
    secure=False
)

# === 2. RabbitMQ connection ===
credentials = pika.PlainCredentials('admin', 'admin123')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
)

channel = connection.channel()

# RabbitMQ setup (must match Spring Boot config)
EXCHANGE = "video.exchange"
QUEUE = "video.process.q"
ROUTING_KEY = "video.process"

# Declare exchange/queue and bind (idempotent, safe if already exists)
channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
channel.queue_declare(queue=QUEUE, durable=True)
channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)


# === 3. Callback for messages ===
def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f"[x] Received task: {data}")

        job_id = data["jobId"]  # ✅ Spring Boot must send this in message
        print(f"Job ID is : {job_id}")
        bucket_name = data["bucket"]
        object_name = data["originalObjectKey"]

        # --- Download path ---
        download_path = os.path.join(BASE_DIR, "downloads", object_name)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        minio_client.fget_object(bucket_name, object_name, download_path)

        # --- Output path ---
        output_path = os.path.join(BASE_DIR, "processed", object_name)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Process
        anonymize_video(download_path, output_path)

        # Upload processed file
        processed_object_name = f"processed/{os.path.basename(object_name)}"
        minio_client.fput_object(bucket_name, processed_object_name, output_path)

        print(f"[✔] Uploaded processed video: {processed_object_name}")

        # ✅ Notify backend that job is complete
        notify_backend(job_id, processed_object_name)

        # Ack message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[!] Error processing: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)




def notify_backend(job_id, processed_file_name):
    url = f"http://localhost:8080/videos/{job_id}/complete"
    data = {
        "processedObjectKey": processed_file_name
    }
    try:
        res = requests.post(url, json=data)
        res.raise_for_status()
        print(f"[✔] Notified backend: job {job_id} -> {processed_file_name}")
    except Exception as e:
        print(f"[!] Failed to notify backend for job {job_id}: {e}")


# === 4. Listen forever ===
channel.basic_consume(
    queue=QUEUE,
    on_message_callback=callback,
    auto_ack=False  # ✅ safer, we ack manually
)

print(f" [*] Waiting for video tasks in '{QUEUE}'. To exit press CTRL+C")
channel.start_consuming()

