from minio import Minio
import json
import io
import os
import uuid
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "data-lake")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def check_bucket_exists(bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} created")
    else:
        logger.info(f"Bucket {bucket_name} already exists")

def convert_number(value):
    if isinstance(value, str):
        value = value.replace(",", ".")
        try:
            return float(value)
        except ValueError:
            return value
    return value

def transform_message(message):
    transformed = {
        "id_zone": message.get("id_zone"),
        "location": {
            "INSEE_C": message.get("INSEE_C"),
            "LIBGEO": message.get("LIBGEO"),
            "EPCI": message.get("EPCI"),
            "DEP": message.get("DEP"),
            "REG": message.get("REG")
        },
        "predictions": {
            "loypredm2": convert_number(message.get("loypredm2")),
            "lwr_IPm2": convert_number(message.get("lwr.IPm2")),
            "upr_IPm2": convert_number(message.get("upr.IPm2")),
            "TYPPRED": message.get("TYPPRED"),
            "observations": {
                "nbobs_com": convert_number(message.get("nbobs_com")),
                "nbobs_mail": convert_number(message.get("nbobs_mail"))
            },
            "R2_adj": convert_number(message.get("R2_adj"))
        }
    }
    return transformed

def save_json_to_minio(json_data, bucket_name):
    file_name = f"{uuid.uuid4().hex}.json"
    data = json.dumps(json_data, indent=4).encode("utf-8")

    client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json"
    )
    logger.info(f"Data stored in MinIO: {file_name}")
