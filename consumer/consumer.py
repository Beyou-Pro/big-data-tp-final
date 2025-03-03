from kafka import KafkaConsumer
import json
import os
import logging
from etl import check_bucket_exists, transform_message, save_json_to_minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "data-lake")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "data-lake")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="data-lake-consumer"
)

def main():
    logger.info(f"Starting Kafka Consumer for topic {TOPIC_NAME} on broker {KAFKA_BROKER}")
    check_bucket_exists(BUCKET_NAME)

    try:
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            transformed_data = transform_message(message.value)
            save_json_to_minio(transformed_data, BUCKET_NAME)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error processing messages: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()
