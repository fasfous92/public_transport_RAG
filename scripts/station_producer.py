import os
import json
import time
import requests
from confluent_kafka import Producer

PRIM_TOKEN = (os.getenv("PRIM_TOKEN", "") or "").strip()
PRIM_BASE = (os.getenv("PRIM_BASE", "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia") or "").rstrip("/")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TRANSPORT_MODE = (os.getenv("TRANSPORT_MODE", "metro") or "metro").strip().lower()

SUPPORTED = {"metro", "bus", "rer"}

TOPICS = {
    "metro": "idf.stations.metro",
    "bus":   "idf.stations.bus",
    "rer":   "idf.stations.rer",
}

# ✅ RER is fetched via these commercial modes (PRIM/Navitia side)
COMMERCIAL_MODES = {
    "metro": ["commercial_mode:Metro"],
    "bus":   ["commercial_mode:Bus"],
    "rer":   ["commercial_mode:RapidTransit", "commercial_mode:LocalTrain", "commercial_mode:RailShuttle", "commercial_mode:regionalRail"],
}

api_key = os.getenv("PRIM_TOKEN")
KAFKA_CONF = {'bootstrap.servers': os.environ.get('KAFKA_SERVER', 'kafka:29092')}
MODES=[{'id': 'commercial_mode:Bus', 'name': 'Bus'},
       {'id': 'commercial_mode:Metro', 'name': 'Métro'},
       {'id': 'commercial_mode:RapidTransit', 'name': 'RER'},
       {'id': 'commercial_mode:Tramway', 'name': 'Tramway'},
]


def _token_value():
    if not PRIM_TOKEN:
        raise RuntimeError("PRIM_TOKEN missing (check .env).")
    t = PRIM_TOKEN.strip()
    if t.lower().startswith("apikey "):
        t = t.split(" ", 1)[1].strip()
    return t


def run_producer():
    p = Producer(KAFKA_CONF)
    topic = "stations"
    
    print(f"🚀 Producer started. Pushing whole batch to '{topic}'")

    for mode in MODES:
        id=mode['id']
        name=mode['name']
        print(f"Fetching stations for mode: {name}")
        
        try:

    
            # 1. Fetch data from API
            url = f'https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/commercial_modes/{id}/stop_areas'
            headers = {"apikey": api_key}
            params = {"count": 1000} 
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            stations = data.get("stop_areas", [])
            try:
                # stations = [
                #         {"id": "stop_area:IDFM:71091", "name": "Vaugirard", "coord": {"lat": "48.840431", "lon": "2.300891"}},
                #         {"id": "stop_area:IDFM:71117", "name": "Vavin", "coord": {"lat": "48.841888", "lon": "2.32926"}},
                #         {"id": "stop_area:IDFM:71315", "name": "Victor Hugo", "coord": {"lat": "48.869747", "lon": "2.28527"}},
                #         {"id": "stop_area:IDFM:478860", "name": "Villejuif - Gustave Roussy", "coord": {"lat": "48.793418", "lon": "2.349241"}},
                #         {"id": "stop_area:IDFM:70143", "name": "Villejuif - Louis Aragon", "coord": {"lat": "48.787638", "lon": "2.366497"}},
                #         {"id": "stop_area:IDFM:70375", "name": "Villejuif L\u00e9o Lagrange", "coord": {"lat": "48.805059", "lon": "2.363903"}},
                #         {"id": "stop_area:IDFM:70248", "name": "Villejuif Paul Vaillant-Couturier", "coord": {"lat": "48.796283", "lon": "2.367631"}},
                #         {"id": "stop_area:IDFM:71403", "name": "Villiers", "coord": {"lat": "48.881522", "lon": "2.315584"}},
                #         {"id": "stop_area:IDFM:71113", "name": "Volontaires", "coord": {"lat": "48.841521", "lon": "2.308241"}},
                #         {"id": "stop_area:IDFM:71750", "name": "Voltaire", "coord": {"lat": "48.858269", "lon": "2.379875"}},
                #         {"id": "stop_area:IDFM:71423", "name": "Wagram", "coord": {"lat": "48.883487", "lon": "2.303995"}}
                #     ]

                if stations is None:
                    raise ValueError("No data fetched from API")
                
             # 1. SEND THE WIPE COMMAND FIRST
                # We include 'mode' so the Sink knows exactly which data to delete
                print(f"🧹 Sending wipe command for mode={name}...")
                
                wipe_msg = {
                    "control": "CLEAR_ALERTS",   # Must match what the Sink is looking for
                    "mode": name            # 'metro', 'bus', or 'rer'
                }
                
                p.produce(topic, value=json.dumps(wipe_msg).encode('utf-8'))
                p.flush() # Ensure the wipe happens before the data arrives
                        
            
                # for station in sta:
                #     entry = {
                #         "id": station["id"],
                #         "name": station["name"],   
                #     }
                #         # produce() is ASYNCHRONOUS - it just adds to a local buffer
                    
                #     p.produce(
                #         topic, 
                #         key=entry["id"], 
                #         value=json.dumps(entry).encode('utf-8'),
                #         callback=delivery_report
                #     )
                #     p.poll(0)
                
                
                
                

                
                print(f"Fetched {len(stations)} stations. Starting Kafka produce...")

                # 2. Queue the entire batch
                for station in stations:
                    entry = {
                        "id": station["id"],
                        "name": station["name"],
                        "coord": {
                            "lat": station["coord"]["lat"],
                            "lon": station["coord"]["lon"]
                        },
                        "mode": name
                    }
                    
                    # produce() is ASYNCHRONOUS - it just adds to a local buffer
                    p.produce(
                        topic, 
                        key=entry["id"], 
                        value=json.dumps(entry).encode('utf-8'),
                        callback=delivery_report
                    )
                    # Serve any pending delivery callbacks
                    p.poll(0)

                # 3. FLUSH ONCE: This sends the whole buffer to Kafka in one go
                print("Flushing batch to broker...")
                p.flush()
                print(f"Batch sent successfully at {time.strftime('%H:%M:%S')}")

            except Exception as e:
                print(f"⚠️ Error during batch: {e}")


        except KeyboardInterrupt:
            print("Stopping producer...")
        finally:
            p.flush()

if __name__ == "__main__":
    main()
