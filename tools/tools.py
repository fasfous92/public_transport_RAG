import os
import re
import json
import unicodedata
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# Env
# ----------------------------
PRIM_TOKEN = (os.getenv("PRIM_TOKEN", "") or "").strip()
PRIM_BASE = (os.getenv("PRIM_BASE", "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia") or "").rstrip("/")
ELASTIC_SERVER = (os.getenv("ELASTIC_SERVER", "http://elasticsearch:9200") or "").rstrip("/")
STATIONS_INDEX = os.getenv("STATIONS_INDEX", "idf-stations")

SUPPORTED_NETWORKS = {"metro", "bus", "rer"}  # allow only these
BLOCKED_KEYWORDS = {"tram", "ter"}           # explicitly not supported


# ----------------------------
# HTTP sessions
# ----------------------------
PRIM = requests.Session()
PRIM.headers.update({"User-Agent": "idf-agent/1.0"})

ES = requests.Session()
ES.headers.update({"User-Agent": "idf-agent/1.0"})


def _strip_quotes(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _prim_headers() -> Dict[str, str]:
    """
    IDFM PRIM/Navitia expects API key in header: apikey: <token>
    (Your tests show Authorization: Apikey ... gives "No API key found")
    """
    tok = _strip_quotes(PRIM_TOKEN)
    if not tok:
        raise RuntimeError("PRIM_TOKEN missing. Put it in .env then rebuild/restart.")
    return {"apikey": tok}


def prim_get(path: str, params: Optional[dict] = None, timeout: int = 30) -> dict:
    url = f"{PRIM_BASE}/{path.lstrip('/')}"
    r = PRIM.get(url, headers=_prim_headers(), params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def es_post(path: str, body: dict, timeout: int = 20) -> dict:
    url = f"{ELASTIC_SERVER}/{path.lstrip('/')}"
    r = ES.post(url, json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ----------------------------
# Normalization helpers
# ----------------------------
def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _contains_blocked(text: str) -> bool:
    t = normalize_text(text)
    return any(k in t for k in BLOCKED_KEYWORDS)


def common_typos_fix(q: str) -> str:
    t = normalize_text(q)
    # common typos you showed
    t = t.replace("billacourt", "billancourt")
    t = t.replace("chhtelet", "chatelet")
    # keep “chatelet” (accent removed already)
    return t


def _extract_line_hint(qn: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (mode_hint, line_hint)
    - mode_hint in {"metro","rer","bus"} or None
    - line_hint: "14" or "A" etc
    """
    mode_hint = None
    line_hint = None

    if "metro" in qn:
        mode_hint = "metro"
        m = re.search(r"\b(\d{1,2})\b", qn)
        if m:
            line_hint = m.group(1)
    elif "rer" in qn:
        mode_hint = "rer"
        m = re.search(r"\b([a-e])\b", qn)  # RER A..E
        if m:
            line_hint = m.group(1).upper()
    elif "bus" in qn:
        mode_hint = "bus"
        m = re.search(r"\b(\d{1,3})\b", qn)
        if m:
            line_hint = m.group(1)

    return mode_hint, line_hint


# ----------------------------
# ES station search (robust to unknown field names)
# ----------------------------
def _pick_id(src: dict, es_id: Optional[str]) -> Optional[str]:
    """
    Try common field names used by station documents.
    We need a Navitia place id like: stop_area:IDFM:xxxxxx
    """
    for k in ["id", "navitia_id", "stop_area_id", "stop_area", "place_id"]:
        v = src.get(k)
        if isinstance(v, str) and v.startswith(("stop_area:", "stop_point:", "poi:", "address:")):
            return v
    # sometimes you stored just the IDFM code; rebuild stop_area format if possible
    for k in ["idfm_id", "idfm", "stop_area_idfm"]:
        v = src.get(k)
        if isinstance(v, str) and v.isdigit():
            return f"stop_area:IDFM:{v}"
    # last resort: ES _id if it already is a navitia id
    if isinstance(es_id, str) and es_id.startswith(("stop_area:", "stop_point:")):
        return es_id
    return None


def _pick_name(src: dict) -> str:
    for k in ["name", "label", "stop_name", "station_name"]:
        v = src.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _pick_mode(src: dict) -> str:
    v = src.get("mode") or src.get("transport_mode") or src.get("commercial_mode")
    if isinstance(v, str):
        return normalize_text(v)
    return ""


def _es_search_stations(query: str, size: int = 5) -> List[dict]:
    q = common_typos_fix(query)
    mode_hint, _ = _extract_line_hint(normalize_text(query))

    # Try to filter by mode if user clearly said metro/bus/rer
    filters = []
    if mode_hint:
        filters.append({"term": {"mode": mode_hint}})

    body = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": [
                                "name^4",
                                "label^3",
                                "stop_name^3",
                                "station_name^3",
                                "city^2",
                                "commune^2",
                                "lines^2",
                                "line_codes^2",
                                "mode^1",
                            ],
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                        }
                    }
                ],
                "filter": filters,
            }
        },
        "_source": True,
    }

    try:
        resp = es_post(f"{STATIONS_INDEX}/_search", body)
        hits = (resp.get("hits") or {}).get("hits") or []
        return hits
    except Exception:
        return []


@lru_cache(maxsize=2048)
def _prim_places_cached(q: str) -> dict:
    # PRIM places endpoint is useful as fallback
    return prim_get("places", params={"q": q})


def _best_place_from_prim(places_json: dict) -> Optional[dict]:
    places = places_json.get("places") or []
    if not places:
        return None

    def score(p: dict) -> float:
        et = p.get("embedded_type") or ""
        quality = float(p.get("quality") or 0)
        base = 1000.0 if et == "stop_area" else 0.0
        return base + quality

    places.sort(key=score, reverse=True)
    return places[0]


def get_station_id(station_name: str) -> str:
    """
    Returns a JSON string:
      {"name": "...", "id": "stop_area:IDFM:...", "mode": "metro|bus|rer", "candidates":[...]}
    """
    if not station_name or not station_name.strip():
        return "Error: empty station name."

    if _contains_blocked(station_name):
        return "Not supported: Tram / TER. Supported: Metro + Bus + RER."

    # 1) ES fuzzy
    hits = _es_search_stations(station_name, size=5)
    candidates = []
    for h in hits:
        src = h.get("_source") or {}
        sid = _pick_id(src, h.get("_id"))
        nm = _pick_name(src)
        md = _pick_mode(src)
        if sid and nm:
            candidates.append({"name": nm, "id": sid, "mode": md or "unknown", "score": h.get("_score", 0)})

    if candidates:
        best = candidates[0]
        payload = {
            "name": best["name"],
            "id": best["id"],
            "mode": best.get("mode") or "unknown",
            "candidates": [{"name": c["name"], "mode": c["mode"]} for c in candidates[:5]],
        }
        return json.dumps(payload, ensure_ascii=False)

    # 2) PRIM fallback
    try:
        q = common_typos_fix(station_name)
        data = _prim_places_cached(q)
        best = _best_place_from_prim(data)
        if not best:
            return f"Station not found: '{station_name}'. Try adding a precision (e.g., 'Châtelet - Les Halles')."

        pid = best.get("id")
        nm = best.get("name")
        if not (isinstance(pid, str) and isinstance(nm, str)):
            return f"Station not found: '{station_name}'."

        payload = {"name": nm, "id": pid, "mode": "unknown", "candidates": []}
        return json.dumps(payload, ensure_ascii=False)
    except requests.HTTPError as e:
        # Don’t crash the app on 401
        return f"PRIM error while resolving station (HTTP). Check PRIM_TOKEN / header 'apikey'. Details: {str(e)}"
    except Exception as e:
        return f"Error while resolving station: {str(e)}"


def _resolve_place_id(name: str) -> str:
    out = get_station_id(name)
    if out.startswith("Error") or out.startswith("Not supported") or out.startswith("Station not found") or out.startswith("PRIM error"):
        raise ValueError(out)
    obj = json.loads(out)
    return obj["id"]


# ----------------------------
# Itinerary formatting
# ----------------------------
def _format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "?"
    m = int(seconds // 60)
    if m < 60:
        return f"{m} min"
    h = m // 60
    mm = m % 60
    return f"{h} h {mm:02d} min"


def _safe_network_name(di: dict) -> str:
    n = di.get("network")
    # sometimes dict, sometimes str (you hit that bug)
    if isinstance(n, dict):
        return (n.get("name") or "").strip()
    if isinstance(n, str):
        return n.strip()
    return ""


def _allowed_section(section: dict) -> bool:
    """
    Allow Metro + Bus + RER. Block Tram/TER.
    """
    if (section.get("type") or "") != "public_transport":
        return True

    di = section.get("display_informations") or {}
    cm = normalize_text(di.get("commercial_mode") or "")
    nm = normalize_text(_safe_network_name(di))
    code = (di.get("code") or di.get("name") or "").strip()

    # hard blocks
    if "tram" in cm or "tram" in nm:
        return False
    if "ter" in cm or "ter" in nm:
        return False

    # allow metro
    if "metro" in cm:
        return True

    # allow bus
    if "bus" in cm:
        return True

    # allow RER: commercial_mode often "RER" or "RapidTransit"
    if "rer" in cm or "rapid" in cm or "transilien" in cm:
        # optionally restrict to A..E if it looks like RER
        if len(code) == 1 and code.upper() in {"A", "B", "C", "D", "E"}:
            return True
        # if code not present, still allow (better than false negative)
        return True

    # default: reject unknown rail modes
    return False


def _format_section(section: dict, idx: int) -> List[str]:
    lines = []
    stype = section.get("type") or ""

    if stype == "street_network":
        dep = ((section.get("from") or {}).get("name") or "").strip()
        arr = ((section.get("to") or {}).get("name") or "").strip()
        dur = _format_duration(section.get("duration"))
        mode = normalize_text(section.get("mode") or "walking")
        label = "Walk" if mode == "walking" else mode.capitalize()
        lines.append(f"{idx}. **{label}** ({dur}) — {dep} → {arr}")
        return lines

    if stype == "public_transport":
        di = section.get("display_informations") or {}
        dep = ((section.get("from") or {}).get("name") or "").strip()
        arr = ((section.get("to") or {}).get("name") or "").strip()
        dur = _format_duration(section.get("duration"))

        cm = (di.get("commercial_mode") or "").strip()
        code = (di.get("code") or "").strip()
        name = (di.get("name") or "").strip()
        direction = (di.get("direction") or "").strip()

        # label
        cmn = normalize_text(cm)
        if "metro" in cmn:
            label = f"Metro {code or name}".strip()
        elif "bus" in cmn:
            label = f"Bus {code or name}".strip()
        else:
            # RER etc
            label = f"{cm} {code or name}".strip()

        lines.append(f"{idx}. **{label}** ({dur})")
        if direction:
            lines.append(f"   - Direction: {direction}")
        lines.append(f"   - {dep} → {arr}")
        return lines

    # fallback
    dep = ((section.get("from") or {}).get("name") or "").strip()
    arr = ((section.get("to") or {}).get("name") or "").strip()
    dur = _format_duration(section.get("duration"))
    lines.append(f"{idx}. **{stype}** ({dur}) — {dep} → {arr}")
    return lines


def _format_journey(journey: dict) -> str:
    dur = _format_duration(journey.get("duration"))
    transfers = journey.get("nb_transfers", 0)
    out = [f"**Total:** {dur} — **Transfers:** {transfers}", ""]

    sections = journey.get("sections") or []
    idx = 1
    for s in sections:
        if (s.get("type") or "") in {"waiting", "transfer"}:
            continue
        if not _allowed_section(s):
            continue
        out.extend(_format_section(s, idx))
        idx += 1

    if idx == 1:
        out.append("No allowed PT section found (Metro/Bus/RER).")
    return "\n".join(out)


def get_itinerary(start_station: str, end_station: str) -> str:
    if not start_station or not end_station:
        return "Error: start_station and end_station are required."
    if _contains_blocked(start_station) or _contains_blocked(end_station):
        return "Not supported: Tram / TER. Supported: Metro + Bus + RER."

    try:
        from_id = _resolve_place_id(start_station)
        to_id = _resolve_place_id(end_station)
    except ValueError as e:
        return str(e)

    try:
        data = prim_get("journeys", params={"from": from_id, "to": to_id, "count": 5})
    except requests.HTTPError as e:
        # Make 401 actionable (you had that)
        return (
            "PRIM request failed (HTTP). If you see 401:\n"
            "- Ensure PRIM_TOKEN is correct in .env\n"
            "- Ensure we send header **apikey: <token>** (this code does)\n"
            "- Rebuild/restart containers so env is injected\n"
            f"\nDetails: {str(e)}"
        )
    except Exception as e:
        return f"Error calling PRIM journeys: {str(e)}"

    journeys = data.get("journeys") or []
    if not journeys:
        return "No journey found. Try more precise stop names."

    # Filter allowed sections (metro/bus/rer)
    filtered = []
    for j in journeys:
        secs = j.get("sections") or []
        if any((s.get("type") == "public_transport" and _allowed_section(s)) for s in secs):
            filtered.append(j)

    use = filtered or journeys

    out = ["## Best options (Metro + Bus + RER)"]
    for i, j in enumerate(use[:3], start=1):
        out.append(f"\n### Option {i}")
        out.append(_format_journey(j))
    return "\n".join(out)


# ----------------------------
# Disruptions
# ----------------------------
def get_disruption_context(user_query: str) -> str:
    q = normalize_text(user_query or "")
    if _contains_blocked(q):
        return "Not supported: Tram / TER. Supported: Metro + Bus + RER."

    mode_hint, line_hint = _extract_line_hint(q)

    # physical_mode tries (PRIM sometimes differs)
    tries = []
    if mode_hint == "metro":
        tries = ["physical_mode:Metro"]
    elif mode_hint == "bus":
        tries = ["physical_mode:Bus"]
    elif mode_hint == "rer":
        tries = ["physical_mode:RapidTransit", "physical_mode:LocalTrain", "physical_mode:regionalRail"]
    else:
        # default: metro first
        tries = ["physical_mode:Metro", "physical_mode:RapidTransit", "physical_mode:Bus"]

    last_err = None
    data = None
    used = None
    for pm in tries:
        try:
            data = prim_get(f"line_reports/physical_modes/{pm}/line_reports")
            used = pm
            break
        except Exception as e:
            last_err = e

    if data is None:
        return f"PRIM disruptions failed. Check PRIM_TOKEN / header apikey. Last error: {str(last_err)}"

    reports = data.get("line_reports") or []
    if not reports:
        return f"No disruption reported for {used.replace('physical_mode:', '')}."

    hits = []
    for rep in reports:
        line = rep.get("line") or {}
        code = (line.get("code") or "").strip()
        name = (line.get("name") or "").strip()
        network = ((line.get("network") or {}).get("name") or "").strip()

        # line filter
        if line_hint:
            if mode_hint == "metro" and code != line_hint and name != line_hint:
                continue
            if mode_hint == "rer" and code.upper() != line_hint.upper():
                continue

        disruptions = rep.get("disruptions") or []
        for d in disruptions:
            sev = ((d.get("severity") or {}).get("name") or "Disruption").strip()
            msg = ""
            messages = d.get("messages") or []
            if messages:
                msg = (messages[0].get("text") or "").strip()
            label = f"{network} {name or code}".strip()
            hits.append(f"- **{label}** — {sev}: {msg}".strip())

    if not hits:
        if mode_hint == "metro" and line_hint:
            return f"No disruption found for Metro {line_hint}."
        if mode_hint == "rer" and line_hint:
            return f"No disruption found for RER {line_hint}."
        return f"No matching disruption for '{user_query}'. Try: 'Metro 14', 'RER A', 'bus'."

    title = f"## Live disruptions ({used.replace('physical_mode:', '')})"
    return title + "\n" + "\n".join(hits[:20])
