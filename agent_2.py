import os
import json
import gradio as gr
from openai import OpenAI
from tools.tools import get_disruption_context, get_station_id, get_itinerary

# 1. SETUP CLIENT (Same as before)
client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=os.environ.get("NVIDIA_API_KEY")
)

# Use the powerful model you selected
MODEL_ID = "meta/llama-3.1-405b-instruct" 

# 2. DEFINE TOOLS (Same as before)
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_disruption_context",
            "description": "Get real-time Paris Metro traffic updates.",
            "parameters": {
                "type": "object",
                "properties": {"user_query": {"type": "string"}},
                "required": ["user_query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_station_id",
            "description": "Resolves a fuzzy station name to its exact API ID and coordinates.",
            "parameters": {
                "type": "object",
                "properties": {"station_name": {"type": "string"}},
                "required": ["station_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_itinerary",
            "description": "Calculates the best route between two stations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_station": {"type": "string"},
                    "end_station": {"type": "string"}
                },
                "required": ["start_station", "end_station"]
            }
        }
    }
]

# 3. CORE LOGIC: The Generator Function
def predict(message, history):
    """
    This function handles the conversation for Gradio.
    It yields updates so the user sees 'Thinking...' steps in real-time.
    """
    
    # A. Build Conversation History for OpenAI
    # Start with System Prompt
    messages = [
        {
            "role": "system", 
            "content": "You are a helpful Paris transport assistant. ALWAYS explain your reasoning before calling tools."
        }
    ]

    # Add past chat history (Gradio format -> OpenAI format)
    for human, assistant in history:
        messages.append({"role": "user", "content": human})
        messages.append({"role": "assistant", "content": assistant})
    
    # Add current message
    messages.append({"role": "user", "content": message})

    # B. The Agent Loop (Multi-turn)
    # We use a loop string to accumulate the "Thought Process" + "Final Answer"
    full_response = ""
    
    while True:
        # 1. Ask Model
        response = client.chat.completions.create(
            model=MODEL_ID, messages=messages, tools=tools, tool_choice="auto"
        )
        msg = response.choices[0].message
        
        # 2. Update UI with the Model's "Thinking"
        if msg.content:
            full_response += f"\n{msg.content}\n"
            yield full_response

        # 3. Stop if no tools called
        if not msg.tool_calls:
            break
        
        # 4. Handle Tools
        messages.append(msg) # Add assistant request to history
        
        for tool_call in msg.tool_calls:
            fn_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)
            
            # Show the user we are working...
            status_msg = f"\n🛠️ **Calling Tool:** `{fn_name}` with args `{args}`...\n"
            full_response += status_msg
            yield full_response 

            # Execute Python Code
            try:
                if fn_name == "get_disruption_context":
                    tool_result = get_disruption_context(args["user_query"])
                elif fn_name == "get_station_id":
                    tool_result = get_station_id(args["station_name"])
                elif fn_name == "get_itinerary":
                    s_station = args.get("start_station")
                    e_station = args.get("end_station")
                    tool_result = get_itinerary(s_station, e_station)
                else:
                    tool_result = f"Error: Tool '{fn_name}' not found."
            except Exception as e:
                tool_result = f"Error executing {fn_name}: {e}"

            # Add result to memory
            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": fn_name,
                "content": str(tool_result),
            })
            
            # Show the user the result was received
            full_response += f"✅ **Result:** Received data from {fn_name}.\n"
            yield full_response

# 4. LAUNCH UI
demo = gr.ChatInterface(
    fn=predict,
    title="🗼 Paris Metro AI Agent",
    description="Ask for routes, traffic updates, or station info. I can plan complex trips!",
    examples=[
        "I want to go from Gare de Lyon to La Defense",
        "Is there any traffic on line 14?",
        "How do I get from Chatelet to Orly Airport?"
    ],
    type="messages" # New Gradio format for better chat handling
)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
