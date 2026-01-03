import os
import requests
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")
NVIDIA_MODEL = os.getenv("NVIDIA_MODEL", "nvidia/nv-embedqa-e5-v5")
INVOKE_URL = "https://integrate.api.nvidia.com/v1/embeddings"

SESSION = requests.Session()


@lru_cache(maxsize=2048)
def _cached_embedding(text: str, input_type: str):
    return _raw_embedding(text, input_type)


def _raw_embedding(text: str, input_type: str):
    if not NVIDIA_API_KEY:
        print("⚠️ Missing NVIDIA_API_KEY")
        return []

    payload = {
        "input": [text],
        "model": NVIDIA_MODEL,
        "input_type": input_type,  # "passage" or "query"
        "encoding_format": "float",
    }

    headers = {
        "Authorization": f"Bearer {NVIDIA_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        r = SESSION.post(INVOKE_URL, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]
    except Exception as e:
        print(f"⚠️ NVIDIA embedding error: {e}")
        return []


def get_nvidia_embedding(text: str, input_type: str = "passage"):
    text = (text or "").strip()
    if not text:
        return []
    # cache only short-ish content to avoid exploding RAM
    if len(text) <= 2000:
        return _cached_embedding(text, input_type)
    return _raw_embedding(text, input_type)
