import os
import json
import time
import unicodedata
import re
import requests
from confluent_kafka import Consumer

ELASTIC_SERVER = (os.getenv("ELASTIC_SERVER", "http://elasticsearch:9200") or "").rstrip("/")
STATIONS_INDEX = (os.getenv("STATIONS_INDEX", "idf-stations") or "").strip()
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
STATIONS_TOPICS = (os.getenv("STATIONS_TOPICS", "idf.stations.metro,idf.stations.bus,idf.stations.rer") or "").split(",")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "stations-sink-v2")
BULK_SIZE = int(os.getenv("BULK_SIZE", "500") or "500")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "idf-stations-sink/1.0", "Accept": "application/json"})


def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def es(method: str, path: str, body=None, timeout=20):
    url = f"{ELASTIC_SERVER}/{path.lstrip('/')}"
    r = SESSION.request(method, url, json=body, timeout=timeout)
    return r.status_code, r.text, (r.json() if r.text and r.headers.get("content-type", "").startswith("application/json") else None)


def ensure_index():
    code, _, _ = es("GET", STATIONS_INDEX)
    if code == 200:
        return

    mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "folding": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "mode": {"type": "keyword"},  # metro/bus/rer
                "embedded_type": {"type": "keyword"},
                "name": {"type": "text", "analyzer": "folding"},
                "name_folded": {"type": "keyword"},
                "label": {"type": "text", "analyzer": "folding"},
                "city": {"type": "text", "analyzer": "folding"},
                "coord": {"type": "object"},
                "ts": {"type": "date"}
            }
        }
    }
    es("PUT", STATIONS_INDEX, mapping)


def bulk_index(actions):
    if not actions:
        return
    payload = "\n".join(actions) + "\n"
    url = f"{ELASTIC_SERVER}/_bulk"
    r = SESSION.post(url, data=payload.encode("utf-8"), headers={"Content-Type": "application/x-ndjson"})
    if r.status_code >= 300:
        raise RuntimeError(f"Bulk error {r.status_code}: {r.text}")


def main():
    ensure_index()

    c = Consumer({
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([t.strip() for t in STATIONS_TOPICS if t.strip()])

    actions = []
    buffered = 0

    print(f"✅ Stations sink running. Topics={STATIONS_TOPICS} index={STATIONS_INDEX} group={GROUP_ID}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                if actions:
                    bulk_index(actions)
                    actions, buffered = [], 0
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            mode = (data.get("mode") or "").strip().lower()

            doc = {
                "id": data.get("id"),
                "mode": mode,
                "embedded_type": data.get("embedded_type", "stop_area"),
                "name": data.get("name"),
                "name_folded": normalize_text(data.get("name", "")),
                "label": data.get("label"),
                "city": data.get("city"),
                "coord": data.get("coord"),
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            doc_id = f"{mode}:{doc['id']}"
            actions.append(json.dumps({"index": {"_index": STATIONS_INDEX, "_id": doc_id}}, ensure_ascii=False))
            actions.append(json.dumps(doc, ensure_ascii=False))
            buffered += 1

            if buffered >= BULK_SIZE:
                bulk_index(actions)
                actions, buffered = [], 0

    finally:
        c.close()


if __name__ == "__main__":
    main()
