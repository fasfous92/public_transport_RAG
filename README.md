

# 🗼 Paris Metro AI Agent (RAG + Tool Calling)

A containerized AI Assistant for Paris Public Transport. This application uses a Large Language Model (**Llama 3.1 405B**) equipped with real-time tools to plan itineraries, check traffic disruptions, and resolve station locations.

The project is built with **Python**, **Gradio**, and **Docker**, and integrates into a larger data pipeline involving **Kafka** and **Elasticsearch**.

---

## 🚀 Features

* **🧠 Intelligent Reasoning:** The agent doesn't just guess; it plans. It explains its thought process before executing actions (e.g., *"I need to find the ID for Gare de Lyon first"*).
* **🗺️ Itinerary Planning:** Calculates the best route between any two stations in Île-de-France.
* **⚠️ Disruption Alerts:** Checks real-time traffic updates for lines and stations.
* **📍 Fuzzy Station Matching:** Understands approximate names (e.g., "defense"  "La Défense").
* **💬 Streamed Responses:** See the agent's "thoughts" and tool outputs in real-time via the Web UI.

---

## Demonstration

https://github.com/user-attachments/assets/a4fcd87d-37d7-4357-9b71-2fdd88007ed9

---

## 🏗️ Architecture

The system runs completely inside **Docker**.

### 1. The Agent Service (`paris-agent`)

* **Interface:** Gradio Web UI (Accessible at `http://localhost:7860`).
* **Brain:** Llama 3.1 405B (via NVIDIA API).
* **Logic:** A custom `while` loop that handles:
1. **User Query**  **LLM Reasoning**
2. **Tool Selection** (Traffic, Itinerary, Station Search)
3. **Execution** (Python functions calling IDFM APIs)
4. **Final Response**



### 2. The Data Pipeline (Background Services)

The agent sits on top of a robust data infrastructure defined in `docker-compose.yml`:

* **Kafka (KRaft Mode):** Handles real-time streams of transport data.
    - Manual update of the stations List (as stations rarly change)
    - Dynamic update of traffic infos (every 5 minutes) 
* **Elasticsearch:** Stores indexed station and alert data for fast retrieval (either with search or with embbeddings)
* **Producers & Sinks:** Python scripts that fetch data from the API and move it between Kafka and Elasticsearch.

--- 


## 🛠️ Prerequisites

* **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** (includes Docker Compose) installed and running.
* **[NVIDIA NIM API Key](https://build.nvidia.com/explore/discover)** (create an account to access Llama 3.1 405B and get API key).
* **[Île-de-France Mobilités (PRIM) API Key](https://prim.iledefrance-mobilites.fr/)** (register for a free account to access real-time transport data).
## ⚙️ Installation & Setup

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd public_transport_RAG

```

---

### 2. Configure Environment Variables

Create a `.env` file in the root directory to store your credentials and configuration safely.

```ini
# .env file

# API Keys
NVIDIA_API_KEY=nvapi-your-key-here
PRIM_TOKEN=your-prim-idfm-token-here

# Producer Configuration
# Set to 'false' for testing (saves API quota) or 'true' for live updates (runs every 5 mins)
CONTINUOUS_RUN=false 
RUN_INTERVAL_SECONDS=300

```

> **Tip:** We recommend keeping `CONTINUOUS_RUN=false` during initial testing to avoid hitting API rate limits.

### 3. Build and Start Services

Build the Docker images and start the core infrastructure (Agent, Kafka, Elasticsearch, and Sinks).

```bash
docker-compose up -d --build

```

**Note:** The data ingestion producers (`station-producer` and `alert-producer`) are **not** started automatically. This is intentional to prevent unnecessary API calls on startup. You must launch them manually after the system is ready.

### 4. Verify Elasticsearch Availability

Elasticsearch takes a few moments to initialize. Before generating data, verify that the database is healthy and ready to accept connections.

Run the following command:

```bash
curl -X GET "localhost:9200/"

```

* **❌ If the connection fails** (e.g., `Connection reset by peer` or `Connection refused`):
Wait 1-2 minutes and try again.
* **✅ If you receive a JSON response** (similar to below), the system is ready:
```json
{
  "name" : "docker-cluster",
  "version" : { ... },
  "tagline" : "You Know, for Search"
}

```



### 5. Initialize Data

Once Elasticsearch is confirmed running, trigger the producers manually to populate the database with the latest station and disruption data.

```bash
docker-compose --profile manual run --rm station-producer
docker-compose --profile manual run --rm alert-producer

```

### 5. Access the App

Open your browser and go to:
👉 **[http://localhost:7860](https://www.google.com/search?q=http://localhost:7860)**

---

## 🖥️ How to Use

1. **Type a request:**
> *"I want to go from Gare de Lyon to La Defense"*


2. **Watch the thought process:**
* The agent will display `🧠 Reflection` explaining its plan.
* It will trigger `🛠️ Tool Request: get_station_id` to find exact coordinates.
* It will trigger `get_itinerary` with those IDs.


3. **Get the Result:** The final answer will summarize the best route.

**Note on Sessions:**
The chat memory is **per-browser-tab**. If you refresh the page, the conversation history is wiped. The agent currently does not store long-term memory in a database.

---

## 📂 Project Structure

```text
.
├── agent.py            # Main application logic (Gradio + LLM Loop)
├── docker-compose.yml       # Orchestration of all services
├── Dockerfile               # Environment definition for the Agent
├── requirements.txt         # Python dependencies
├── .env                     # API Keys (Not committed to Git)
├── tools/
│   └── tools.py         # Python functions wrapping the APIs
├── scripts/                    # Data Pipeline scripts
│   ├── station_producer.py     # Fetches stations data -> Kafka
│   ├── disturbance_producer.py # Fetches traffic data -> Kafka
│   ├── disturbance_sink.py     # Kafka-> embeddings-> Elasticsearch
│   └── stations_sink.py        # Kafka -> Elasticsearch
└── README.md

```

---

## 🔧 Troubleshooting

**1. "400 Bad Request" / API Errors**

* Ensure your `agent.py` includes the `sanitize_content` function. OpenAI/NVIDIA APIs require strings, not Gradio dictionaries.

**2. Network or Connection Refused**

* Check if containers are running: `docker compose ps`
* View logs: `docker compose logs -f paris-agent`

**3. "Unauthorized" Tool Calls**

* Verify that `PRIM_TOKEN` is correctly mapped in `docker-compose.yml` under the `paris-agent` service environment variables.
