import json
import os
import sys
import time
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch

try:
    from tools.nvidia_embedding import get_nvidia_embedding
except ImportError:
    print("❌ Error: Could not import 'tools.nvidia_embedding'")
    sys.exit(1)

VECTOR_DIMS = 1024
INDEX_NAME = "paris-disruptions"

KAFKA_CONF = {
    "bootstrap.servers": os.getenv("KAFKA_SERVER", "kafka:29092"),
    "group.id": "nvidia-disruptions-sink-v2",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "session.timeout.ms": 6000,
}

def create_index_if_not_exists(es: Elasticsearch):
    if es.indices.exists(index=INDEX_NAME):
        return

    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "mode": {"type": "keyword"},
                "physical_mode": {"type": "keyword"},
                "status": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "updated_at": {"type": "date"},
                "embedding_vector": {
                    "type": "dense_vector",
                    "dims": VECTOR_DIMS,
                    "index": True,
                    "similarity": "cosine",
                },
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"✅ Index '{INDEX_NAME}' created.")

def run_sink():
    es = Elasticsearch(os.getenv("ELASTIC_SERVER", "http://elasticsearch:9200"), meta_header=False)

    while True:
        try:
            if es.ping():
                break
        except:
            pass
        print("⏳ Waiting for Elasticsearch...")
        time.sleep(3)

    create_index_if_not_exists(es)
    print("🚀 Disruptions Sink Started...")

    while True:
        try:
            consumer = Consumer(KAFKA_CONF)
            consumer.subscribe([INDEX_NAME])  # topic is "paris-disruptions"
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"❌ Kafka Error: {msg.error()}")
                    break

                data = json.loads(msg.value().decode("utf-8"))

                # wipe only one mode
                if data.get("control") == "CLEAR_ALERTS":
                    mode = data.get("mode")
                    if mode:
                        print(f"🧹 Clearing disruptions for mode={mode} ...")
                        es.delete_by_query(
                            index=INDEX_NAME,
                            body={"query": {"term": {"mode": mode}}},
                            conflicts="proceed",
                        )
                    else:
                        print("🧹 Clearing ALL disruptions ...")
                        es.delete_by_query(index=INDEX_NAME, body={"query": {"match_all": {}}}, conflicts="proceed")
                    continue

                text_to_embed = f"{data.get('title','')} {data.get('description','')}".strip()
                vector = None
                try:
                    vector = get_nvidia_embedding(text_to_embed, input_type="passage")
                except Exception as e:
                    print(f"⚠️ Embedding Failed: {e}")

                if vector:
                    data["embedding_vector"] = vector
                    doc_id = f"{data.get('id')}::{data.get('mode')}"
                    es.index(index=INDEX_NAME, id=doc_id, document=data)
                    # print(f"✅ Indexed: {data.get('title')} ({data.get('mode')})")
                else:
                    print(f"⚠️ Skipped indexing (No vector): {data.get('title')}")

        except Exception as e:
            print(f"💥 Critical Error: {e}. Retrying in 5s...")
            time.sleep(5)
        finally:
            try:
                consumer.close()
            except:
                pass

if __name__ == "__main__":
    run_sink()
