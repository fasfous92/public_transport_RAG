import os
from elasticsearch import Elasticsearch
from tools.nvidia_embedding import get_nvidia_embedding
import requests

# Connect to the same Docker container as your Sink
es = Elasticsearch("http://elasticsearch:9200", meta_header=False)
INDEX_NAME_DISRUBPTIONS = "paris-disruptions"
INDEX_NAME_STATION = "stations"
PRIM_TOKEN = os.environ.get("PRIM_TOKEN")


def get_disruption_context(user_query: str) -> str:
    """
    Searches the live disruption database using Vector Search.
    Returns a clean, human-readable string for the LLM.
    """
    try:
        
            # 1. Vectorize with "query" type
        query_vector = get_nvidia_embedding(user_query, input_type="query")
        
        if not query_vector:
            return "Error: Could not generate embedding from API."

        # 2. Search Elastic (Same logic as before)
        response = es.search(
            index=INDEX_NAME_DISRUBPTIONS,
            knn={
                "field": "embedding_vector",
                "query_vector": query_vector,
                "k": 3,
                "num_candidates": 50
            }
        )
        
        # 3. Format the Output for the LLM
        hits = response['hits']['hits']
        
        if not hits:
            return "No active disruptions found regarding your query."

        # We construct a clean text block. No JSON formatting for the LLM.
        context_text = "Here are the active disruptions found:\n"
        for i, hit in enumerate(hits, 1):
            source = hit['_source']
            score = hit['_score'] # How relevant is this? (Optional)
            
            context_text += (
                f"{i}. [Line {source.get('impact')}] {source.get('title')}\n"
                f"   Details: {source.get('description')}\n"
                f"   Severity: {source.get('severity')}\n\n"
            )

        return context_text

    except Exception as e:
        return f"Error retrieving context: {str(e)}"


def get_station_id(station_name: str):
    """
    Finds a station ID using Fuzzy Text Search (Tolers typos).
    No API costs.
    """
    print(f"🔎 Looking up station (Text Search): '{station_name}'...")

    try:
        # Standard Search with Fuzziness
        response = es.search(
            index=INDEX_NAME_STATION,
            query={
                "match": {
                    "name": {
                        "query": station_name,
                        "fuzziness": "AUTO", # Auto-calculates typo tolerance
                        "operator": "and"
                    }
                }
            },
            size=1
        )

        hits = response['hits']['hits']
        if not hits:
            return "No station found matching that name. ask the user for clarification."

        top_hit = hits[0]['_source']
        
        return {
            "id": top_hit.get("id"),
            "name": top_hit.get("name"),
            "coordinates": top_hit.get("coord")
        }

    except Exception as e:
        return f"Error: {str(e)}"
# --- MOCK USAGE (What your Agent calls) ---





def extract_best_journeys(raw_data):
    """
    Parses the raw Navitia API response into a clean, agent-readable format.
    """
    if not raw_data or 'journeys' not in raw_data:
        return "No journeys found."

    # 1. Map global disruptions for quick lookup
    disruptions_map = {}
    for d in raw_data.get("disruptions", []):
        for obj in d.get("impacted_objects", []):
            line_id = obj.get("pt_object", {}).get("id")
            msg = next((m['text'] for m in d.get('messages', []) if m.get('channel', {}).get('name') == 'titre'), "Perturbation")
            disruptions_map[line_id] = {
                "effect": d.get("severity", {}).get("effect"),
                "message": msg
            }

    processed_journeys = []

    # 2. Process the first 3 journeys
    # (Fixed bug: changed [:0] to [:3])
    for i, journey in enumerate(raw_data['journeys'][:2]):
     
        # Calculate duration in minutes
        duration_mins = journey.get('duration', 0)//60
        
        journey_info = [
            f"Option {i + 1}: {duration_mins} mins ({journey.get('nb_transfers', 0)} transfers)",
            f"   Departure: {journey.get('departure_date_time')[9:11]}:{journey.get('departure_date_time')[11:13]}",
            f"   Arrival:   {journey.get('arrival_date_time')[9:11]}:{journey.get('arrival_date_time')[11:13]}",
            "   Itinerary:"
        ]

        # 3. Extract steps
        for section in journey.get("sections", []):
            mode = section.get("type")
            
            if mode == "waiting":
                continue
                
            step_duration = section.get('duration', 0)//60
            from_name = section.get("from", {}).get("name", "Origin")
            to_name = section.get("to", {}).get("name", "Dest")

            if mode == "public_transport":
                info = section.get("display_informations", {})
                line_code = info.get("code", "?")
                direction = info.get("direction", "Unknown")
                line=info.get('physical_mode','line')
                # Check for disruption on this specific line leg
                alert_msg = ""
                # Trying to find if this link ID is in our disruption map
                for link in section.get("links", []):
                    if link.get("id") in disruptions_map:
                        alert_msg = f" ⚠️ ALERT: {disruptions_map[link['id']]['message']}"
                journey_info.append(f"    - Take {line} {line_code} towards {direction} ({step_duration} min) : {alert_msg}")
            
            elif mode == "street_network" or mode == "walking":
                journey_info.append(f"    - Walk from {from_name} to {to_name} ({step_duration} min)")
            
            elif mode == "transfer":
                journey_info.append(f"    - Transfer at {from_name} ({step_duration} min)")

        processed_journeys.append("\n".join(journey_info))

    return "\n\n".join(processed_journeys)

def get_itinerary(start_station: str, end_station: str):
    """
    Calculates the best route between two stations using the IDFM API.
    Args:
        start_station: Name of the departure station (e.g. "Gare de Lyon")
        end_station: Name of the destination (e.g. "La Defense")
    """
    print(f"🗺️  Planning trip: {start_station} -> {end_station}")
    
    # A. Resolve Coordinates
    start_station_data = get_station_id(start_station) #get_coords_from_name(start_station)
    end_station_data = get_station_id(end_station) #get_coords_from_name(end_station)
    
    if isinstance(start_station_data, str):
        return f"Error: Could not find coordinates for '{start_station}'. Please try a more specific name."
    if isinstance(end_station_data, str):
        return f"Error: Could not find coordinates for '{end_station}'. Please try a more specific name."

    # B. Call API
    start_coord = f"{start_station_data['coordinates']['lon']};{start_station_data['coordinates']['lat']}"
    end_coord = f"{end_station_data['coordinates']['lon']};{end_station_data['coordinates']['lat']}"

    base_url = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/journeys"

    # 2. Build the URL manually
    # (We do this because 'requests' would wrongly convert the ';' into '%3B')
    full_url = f"{base_url}?from={start_coord}&to={end_coord}"
    headers = {"apikey": PRIM_TOKEN}


    try:
        response= requests.get(full_url, headers=headers, timeout=10)
        response.raise_for_status()
        response=response.json()

        # C. Parse Results
        return extract_best_journeys(response)
        
    except Exception as e:
        return f"tool API Error: {str(e)}"



if __name__ == "__main__":
    # Simulate a user question
    # query = "Is the metro to the airport working?"
    # print(f"🔎 User asks: '{query}'\n")
    
    # context = get_disruption_context(query)
    # print("-" * 20)
    # print("CONTEXT RETRIEVED FOR LLM:")
    # print(context)
    # print("-" * 20)

    #get station ID example
    print(get_station_id("Gare de lyon"))   # Perfect match
    print(get_station_id("Gare de lion"))   # Typo -> Should still work!
    print(get_station_id("st lazare"))   # Partial name -> Should still work!
    print(get_itinerary("Gare de Lyon", "La Defense"))
