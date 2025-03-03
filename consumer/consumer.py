from kafka import KafkaConsumer
import json
import pandas as pd
import os
from minio import Minio
from io import BytesIO
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "data-lake")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "data-lake")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


def main():
    logger.info(f"Starting consumer for topic {TOPIC_NAME} on broker {KAFKA_BROKER}")
    logger.info(f"MinIO config: endpoint={MINIO_ENDPOINT}, bucket={BUCKET_NAME}")

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logger.info(f"Created bucket: {BUCKET_NAME}")
        else:
            logger.info(f"Bucket {BUCKET_NAME} already exists")

    except Exception as e:
        logger.error(f"MinIO connection error: {e}")
        return

    try:
        logger.info("Connecting to Kafka...")
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='data-lake-consumer',
            consumer_timeout_ms=30000
        )
        logger.info("Successfully connected to Kafka")

    except Exception as e:
        logger.error(f"Kafka connection error: {e}")
        return

    data_list = []
    message_count = 0

    try:
        logger.info("Starting to consume messages...")
        for message in consumer:
            message_count += 1
            data_list.append(message.value)

            if len(data_list) >= BATCH_SIZE:
                save_batch_to_minio(client, data_list)
                data_list = []

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error processing messages: {e}")
    finally:
        if data_list:
            save_batch_to_minio(client, data_list)

        consumer.close()
        logger.info("Kafka connection closed.")


def save_batch_to_minio(client, data_list):
    try:
        df = pd.DataFrame(data_list)
        csv_data = df.to_csv(index=False).encode('utf-8')
        file_name = f"data_{uuid.uuid4().hex}.csv"

        client.put_object(
            BUCKET_NAME, file_name, BytesIO(csv_data), len(csv_data), content_type="text/csv"
        )
        logger.info(f"File {file_name} saved to MinIO with {len(data_list)} records")
    except Exception as e:
        logger.error(f"Error saving to MinIO: {e}")


if __name__ == "__main__":
    main()