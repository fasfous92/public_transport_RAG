import os
import json
import time
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch

# --- CONFIG ---
INDEX_NAME = "stations"
KAFKA_CONF = {
    'bootstrap.servers': os.environ.get('KAFKA_SERVER', 'kafka:29092'),
    'group.id': 'stations-sink-text-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

def create_index_if_not_exists(es):
    if not es.indices.exists(index=INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    # Standard text field with fuzzy capabilities
                    "name": {"type": "text", "analyzer": "standard"}, 
                    "coordinates": {"type": "geo_point"},
                    "mode": {"type": "keyword"}
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

        try:
            val = msg.value()
            if not val: continue
            data = json.loads(val.decode('utf-8'))
            
         # 1. WIPE SIGNAL
            if data.get("control") == "CLEAR_ALERTS":
                mode_to_wipe = data.get("mode")

                if mode_to_wipe:
                    print(f"🧹 Clearing alerts for mode={mode_to_wipe}...")
                    es.delete_by_query(
                        index=INDEX_NAME,
                        body={"query": {"term": {"mode": mode_to_wipe}}},
                        conflicts="proceed"
                    )
                else:
                    print("🧹 Clearing ALL alerts (no mode specified)...")
                    es.delete_by_query(
                        index=INDEX_NAME, 
                        body={"query": {"match_all": {}}},
                        conflicts="proceed"
                    )
                continue

            # Just index the raw data. No API calls needed!
            es.index(index=INDEX_NAME, id=data['name'], document=data)
            print(f"✅ Indexed: {data.get('name')}")


if __name__ == "__main__":
    main()
