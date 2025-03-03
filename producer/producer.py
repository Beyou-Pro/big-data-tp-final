from kafka import KafkaProducer
import json
import pandas as pd
import requests
import os
from io import StringIO

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "data-lake"

DATA_URLS = [
    "https://www.data.gouv.fr/fr/datasets/r/dfb542cd-a808-41e2-9157-8d39b5c24edb"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for url in DATA_URLS:
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(StringIO(response.text), sep=';', dtype=str)
        for _, row in df.iterrows():
            message = row.to_dict()
            producer.send(TOPIC_NAME, message)
            print(f"Envoyé : {message}")
    else:
        print(f"Erreur lors du téléchargement de {url}")

producer.flush()
print("Tous les messages ont été envoyés à Kafka.")
