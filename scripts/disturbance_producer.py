import json
import os
import time
import requests
from datetime import datetime
from confluent_kafka import Producer

# --- CONFIG ---
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka:29092')
TOPIC = "paris-disruptions-metro"
PRIM_TOKEN = os.environ.get('PRIM_TOKEN')

# 🟢 NEW: Boolean Toggle for Loop vs Run-Once
CONTINUOUS_RUN = os.getenv("CONTINUOUS_RUN", "false").strip().lower() == "true"
LOOP_INTERVAL_SECONDS = os.getenv("LOOP_INTERVAL_SECONDS", "300")
Modes = [
            {'id': 'physical_mode:Bus','name': 'Bus'},
            {'id': 'physical_mode:Metro','name': 'Metro'},
            {'id': 'physical_mode:RapidTransit','name': 'RER'},
            {'id': 'physical_mode:Tramway','name': 'Tramway'}
        ]

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
    
    for mode in Modes:
        id = mode['id']
        name = mode['name']
        print(f"Fetching disruptions for mode: {name}")
        
        p = Producer({'bootstrap.servers': KAFKA_SERVER})
        

        # 2. FETCH FROM API
        url = f"https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/physical_modes/{id}/line_reports"
        headers = {"apikey": PRIM_TOKEN}
        params = {} 

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            response_json = response.json()
            
            # 1. SEND THE WIPE COMMAND FIRST
            # We include 'mode' so the Sink knows exactly which data to delete
            print(f"🧹 Sending wipe command for mode={name}...")
            
            wipe_msg = {
                "control": "CLEAR_ALERTS",   # Must match what the Sink is looking for
                "mode": name                 # 'metro', 'bus', 'rer', etc.
            }
            
            p.produce(TOPIC, value=json.dumps(wipe_msg).encode('utf-8'))
            p.flush() # Ensure the wipe happens before the data arrives
                    

            disruptions_cleaned = []

            # Navigate the Navitia structure: Reports -> Disruptions
            for disruption in response_json.get("disruptions", []):
                
                # Filter for 'Actualité' tags as requested
                if 'Actualité' in disruption.get('tags', []):
                    title = ""
                    description = ""
                    
                    # Extract text based on channel types
                    for message in disruption.get('messages', []):
                        channels = message.get('channel', {}).get('types', [])
                        if 'title' in channels:
                            title = clean_text(message.get('text'))
                        elif 'web' in channels:
                            description = clean_text(message.get('text'))

                    # Build the LLM-optimized document
                    to_copy = {
                        'id': disruption.get('id'),
                        'status': disruption.get('status'),
                        'period': disruption.get('application_periods', [{}])[0],
                        'severity': disruption.get('severity', {}).get('name'),
                        'title': title,
                        'description': description,
                        'updated_at': datetime.now().isoformat(),
                        'mode': name
                    }
                    
                    # Push to Kafka immediately
                    payload = json.dumps(to_copy, ensure_ascii=False).encode('utf-8')
                    p.produce(TOPIC, key=to_copy['id'], value=payload)
                    disruptions_cleaned.append(to_copy)

            p.flush()
            print(f"✅ Successfully processed {len(disruptions_cleaned)} 'Actualité' alerts for {name}.")

        except Exception as e:
            print(f"❌ Producer Error for {name}: {e}")

if __name__ == "__main__":
    if CONTINUOUS_RUN:
        print(f"🔄 CONTINUOUS_RUN=true. Starting loop every {LOOP_INTERVAL_SECONDS} seconds...")
        while True:
            try:
                run_producer()
                print(f"✅ Cycle completed. Sleeping for {LOOP_INTERVAL_SECONDS}s...")
            except Exception as e:
                print(f"💥 Critical Error in main loop: {e}")
            
            time.sleep(LOOP_INTERVAL_SECONDS)
    else:
        print("▶️ CONTINUOUS_RUN=false. Running once and exiting...")
        run_producer()
        print("👋 Done.")
        exit(0)

