import json
import os
import time
import requests
from datetime import datetime
from confluent_kafka import Producer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
PRIM_TOKEN = os.getenv("PRIM_TOKEN")
TOPIC = "paris-disruptions"
TRANSPORT_MODE = os.getenv("TRANSPORT_MODE", "metro").strip().lower()

HEADERS = {"apikey": PRIM_TOKEN} if PRIM_TOKEN else {}

# Traffic Info API endpoint you already use
BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/physical_modes"

WIPE_MODE = os.getenv("WIPE_MODE", "1").strip() == "1"

# For disruptions we use PHYSICAL modes
PHYSICAL_MODE_CANDIDATES = {
    "metro": ["Metro"],
    "bus": ["Bus"],
    # RER may be RapidTransit and/or Train depending on coverage
    "rer": ["RapidTransit", "Train"],
}

def clean_text(text):
    if not text:
        return ""
    return (
        text.replace("<br/>", " ")
            .replace("<b>", "")
            .replace("</b>", "")
            .replace("<p>", "")
            .replace("</p>", "")
            .strip()
    )

def fetch_line_reports(physical_mode: str):
    url = f"{BASE_URL}/physical_mode:{physical_mode}/line_reports"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_title_and_description(disruption):
    title = ""
    description = ""
    for message in disruption.get("messages", []):
        channels = message.get("channel", {}).get("types", [])
        if "title" in channels:
            title = clean_text(message.get("text"))
        elif "web" in channels:
            description = clean_text(message.get("text"))
    return title, description

def run_producer():
    if not PRIM_TOKEN:
        raise RuntimeError("PRIM_TOKEN is missing. Put it in .env and restart.")

    p = Producer({"bootstrap.servers": KAFKA_SERVER})

    # wipe only this mode
    if WIPE_MODE:
        p.produce(TOPIC, value=json.dumps({"control": "CLEAR_ALERTS", "mode": TRANSPORT_MODE}).encode("utf-8"))
        p.flush()

    physical_modes = PHYSICAL_MODE_CANDIDATES.get(TRANSPORT_MODE, PHYSICAL_MODE_CANDIDATES["metro"])
    print(f"🔎 transport_mode={TRANSPORT_MODE} -> trying physical_modes={physical_modes}")

    sent = 0
    seen = set()

    for pm in physical_modes:
        try:
            response_json = fetch_line_reports(pm)

            for disruption in response_json.get("disruptions", []):
                
                if "Actualité" not in disruption.get("tags", []):
                    continue

                did = disruption.get("id")
                if not did:
                    continue

                # de-dup if same disruption appears in both RapidTransit and Train
                unique_key = f"{did}::{TRANSPORT_MODE}"
                if unique_key in seen:
                    continue
                seen.add(unique_key)

                title, description = extract_title_and_description(disruption)

                doc = {
                    "id": did,
                    "mode": TRANSPORT_MODE,
                    "physical_mode": pm,
                    "status": disruption.get("status"),
                    "period": (disruption.get("application_periods") or [{}])[0],
                    "severity": (disruption.get("severity") or {}).get("name"),
                    "title": title,
                    "description": description,
                    "updated_at": datetime.now().isoformat(),
                }

                p.produce(
                    TOPIC,
                    key=f"{doc['id']}::{doc['mode']}",
                    value=json.dumps(doc, ensure_ascii=False).encode("utf-8"),
                )
                sent += 1

            p.flush()
            time.sleep(0.1)

        except Exception as e:
            print(f"⚠️ Fetch failed for physical_mode={pm}: {e}")

    print(f"✅ Published {sent} disruptions for mode={TRANSPORT_MODE} using {physical_modes}")

if __name__ == "__main__":
    run_producer()
