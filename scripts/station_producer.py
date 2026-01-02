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

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "idf-station-producer/1.0", "Accept": "application/json"})


def _token_value():
    if not PRIM_TOKEN:
        raise RuntimeError("PRIM_TOKEN missing (check .env).")
    t = PRIM_TOKEN.strip()
    if t.lower().startswith("apikey "):
        t = t.split(" ", 1)[1].strip()
    return t


def prim_get(path: str, params=None, timeout=30):
    url = f"{PRIM_BASE}/{path.lstrip('/')}"
    token = _token_value()

    headers_try = [
        {"apikey": token},
        {"apiKey": token},
        {"Authorization": f"Apikey {token}"},  # fallback
    ]

    last = None
    for hdr in headers_try:
        try:
            r = SESSION.get(url, headers={**SESSION.headers, **hdr}, params=params or {}, timeout=timeout)
            if r.status_code == 401:
                last = Exception(f"401 Unauthorized (tried {list(hdr.keys())[0]})")
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
    raise last


def wait_kafka(prod: Producer, timeout_s=60):
    t0 = time.time()
    while True:
        try:
            md = prod.list_topics(timeout=5)
            if md and md.brokers:
                return
        except Exception:
            pass
        if time.time() - t0 > timeout_s:
            raise RuntimeError("Kafka not ready after timeout.")
        time.sleep(2)


def fetch_stop_areas(commercial_mode: str):
    out = []
    start_page = 0
    while True:
        data = prim_get(
            f"commercial_modes/{commercial_mode}/stop_areas",
            params={"count": 1000, "start_page": start_page}
        )
        sas = data.get("stop_areas", []) or []
        out.extend(sas)
        if len(sas) < 1000:
            break
        start_page += 1
    return out


def main():
    if TRANSPORT_MODE not in SUPPORTED:
        raise RuntimeError(f"Unsupported TRANSPORT_MODE={TRANSPORT_MODE}. Use metro, bus, rer.")

    topic = TOPICS[TRANSPORT_MODE]
    cms = COMMERCIAL_MODES[TRANSPORT_MODE]

    prod = Producer({"bootstrap.servers": KAFKA_SERVER})
    wait_kafka(prod)

    print(f"🔎 transport_mode={TRANSPORT_MODE} -> trying commercial_modes={cms}")

    published = 0
    used = []

    for cm in cms:
        try:
            sas = fetch_stop_areas(cm)
            if not sas:
                continue
            used.append(cm)
            for sa in sas:
                msg = {
                    "id": sa.get("id"),
                    "name": sa.get("name"),
                    "label": sa.get("label"),
                    "mode": TRANSPORT_MODE,
                    "embedded_type": "stop_area",
                    "coord": sa.get("coord"),
                    "city": (sa.get("administrative_regions") or [{}])[0].get("name") if sa.get("administrative_regions") else None,
                }
                prod.produce(topic, value=json.dumps(msg, ensure_ascii=False).encode("utf-8"))
                published += 1
            prod.flush()
        except Exception as e:
            print(f"⚠️ Fetch failed for commercial_mode={cm}: {e}")

    if not used:
        print("❌ No commercial_mode succeeded. Nothing published.")
        return

    print(f"✅ Published {published} stations for mode={TRANSPORT_MODE} using {used}")


if __name__ == "__main__":
    main()
