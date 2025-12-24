import os
import json
from openai import OpenAI
from tools.tools import get_disruption_context, get_station_id, get_itinerary# Import your RAG tool

# 1. SETUP CLIENT
client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=os.environ.get("NVIDIA_API_KEY")
)

MODEL_ID = "meta/llama-3.1-70b-instruct"
MODEL_ID = "meta/llama-3.1-nemotron-70b-instruct"
MODEL_ID = "nvidia/llama-3.3-nemotron-super-49b-v1.5"
# 2. DEFINE TOOLS (OpenAI Format)
tools = [
    # 1. Traffic/Disruption Search
    {
        "type": "function",
        "function": {
            "name": "get_disruption_context",
            "description": "Get real-time Paris Metro traffic updates. Use this for questions about delays, strikes, or general traffic status.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_query": {
                        "type": "string",
                        "description": "The search topic (e.g. 'Line 14 status', 'Orly airport access')"
                    }
                },
                "required": ["user_query"]
            }
        }
    },
    
    # 2. Station Name Resolver
    {
        "type": "function",
        "function": {
            "name": "get_station_id",
            "description": "Resolves a fuzzy station name (e.g., 'Gare de Lyon') to its exact API ID and coordinates. Always use this before calculating an itinerary to ensure the station exists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "station_name": {
                        "type": "string",
                        "description": "The name of the station to look up (e.g., 'Chatelet', 'La Defense')"
                    }
                },
                "required": ["station_name"]
            }
        }
    },

    # 3. Itinerary Calculator (The function we just fixed)
    {
        "type": "function",
        "function": {
            "name": "get_itinerary",
            "description": "Calculates the best route between two stations using the IDFM API. Requires exact station names or IDs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_station": {
                        "type": "string",
                        "description": "The departure station name (e.g. 'Gare du Nord')"
                    },
                    "end_station": {
                        "type": "string",
                        "description": "The destination station name (e.g. 'Stade de France')"
                    }
                },
                "required": ["start_station", "end_station"]
            }
        }
    }
]
# tools= [
#     # 1. Traffic/Disruption Search
#     {
#         "type": "function",
#         "function": {
#             "name": "get_disruption_context",
#             "description": "Get real-time Paris Metro traffic updates. Use this for questions about delays, strikes, or general traffic status.",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "user_query": {
#                         "type": "string",
#                         "description": "The search topic (e.g. 'Line 14 status', 'Orly airport access')"
#                     }
#                 },
#                 "required": ["user_query"]
#             }
#         }
#     },
#     # 2. Station Name Resolver (NEW)
#     {
#         "type": "function",
#         "function": {
#             "name": "get_station_id",
#             # ⚠️ CRITICAL INSTRUCTION ADDED HERE:
#             "description": "Get's a station ID and coordinates ",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "station_name": {
#                         "type": "string",
#                         "description": "The name of the station (try to resolve a vague or typoed station name to its official ID and explicit name. (e.g. Converts 'St Lazare' -> 'Gare Saint-Lazare')"
#                     }
#                 },
#                 "required": ["station_name"]
#             }
#         }
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_itinerary",
#             "description": "Calculate the best route/itinerary between two stations.",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "start_station": {"type": "string", "description": "Departure station name"},
#                     "end_station": {"type": "string", "description": "Destination station name"}
#                 },
#                 "required": ["start_station", "end_station"]
#             }
#         }
#     }
# ]
def run_agent(user_input):
    print(f"🤖 User: {user_input}")
    
    messages = [
        {
            "role": "system", 
            "content": "You are a helpful public transport assistant. ALWAYS keep the same name for the transport means (Metro 1 is different from Line 1)"    
        },
        {"role": "user", "content": user_input}
    ]

    # Start the conversation loop
    while True:
        # 1. Ask the Model
        response = client.chat.completions.create(
            model=MODEL_ID, messages=messages, tools=tools, tool_choice="auto"
        )
        msg = response.choices[0].message
        
        # 2. Print Reasoning (The "Think" block)
        if msg.content:
            print(f"\n🧠 Reflection: {msg.content}")

        # 3. CHECK: Is the model finished? (No tool calls)
        if not msg.tool_calls:
            # If no tools are called, this is the final answer. Return it.
            return msg.content
        
        # 4. If tools ARE called, execute them
        print(f"🛠️  Tool Request: {msg.tool_calls[0].function.name}")
        messages.append(msg) # Add the model's request to history
        
        for tool_call in msg.tool_calls:
            fn_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)
            print(f"   Running: {fn_name}({args})")
            
            # --- Execute Python Code ---
            try:
                if fn_name == "get_disruption_context":
                    tool_result = get_disruption_context(args["user_query"])
                elif fn_name == "get_station_id":
                    tool_result = get_station_id(args["station_name"])
                elif fn_name == "get_itinerary":
                    tool_result = get_itinerary(args.get("start_station"), args.get("end_station"))
                else:
                    tool_result = f"Error: Tool '{fn_name}' not found."
            except Exception as e:
                tool_result = f"Error executing {fn_name}: {e}"

            # --- Add Result to History ---
            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": fn_name,
                "content": str(tool_result),
            })
            
        # 5. Loop continues... (Goes back to step 1 with the new tool outputs)
# def run_agent(user_input):
#     print(f"🤖 User: {user_input}")
    
#     # ### NEW 1: Add a System Prompt to force "Chain of Thought" reasoning
#     messages = [
#         {
#             "role": "system", 
#             "content": "You are a helpful assistant. ALWAYS explain your reasoning and step-by-step plan effectively to the user BEFORE calling any tools."
#         },
#         {"role": "user", "content": user_input}
#     ]

#     # First Call
#     response = client.chat.completions.create(
#         model=MODEL_ID, messages=messages, tools=tools, tool_choice="auto"
#     )
    
#     msg = response.choices[0].message
    
#     # ### NEW 2: Print the content (The "Reflection")
#     # Even when tools are called, Llama 3.1 often puts its reasoning in 'msg.content'
#     if msg.content:
#         print(f"\n🧠 Model Reflection:\n{msg.content}\n")

#     # Check if tool is called
#     if msg.tool_calls:
#         print(f"🛠️  Tool Call Detected: {msg.tool_calls[0].function.name}")
#         messages.append(msg) # Add 'assistant' message
        
#         for tool_call in msg.tool_calls:
#             fn_name = tool_call.function.name
#             args = json.loads(tool_call.function.arguments)
#             print(f"🛠️  Agent Calling: {fn_name}({args})")
            
#             # --- 2. EXECUTE PYTHON CODE ---
#             if fn_name == "get_disruption_context":
#                 tool_result = get_disruption_context(args["user_query"])
#             elif fn_name == "get_station_id":
#                 tool_result = get_station_id(args["station_name"])
#             elif fn_name == "get_itinerary":
#                 # ### NEW 3: Handle potential missing args or wrong names gracefully
#                 s_station = args.get("start_station")
#                 e_station = args.get("end_station")
#                 tool_result = get_itinerary(s_station, e_station)
#             else:
#                 tool_result = f"Error: Tool '{fn_name}' not found."

#             # --- 3. FEEDBACK TO AGENT ---
#             messages.append({
#                 "tool_call_id": tool_call.id,
#                 "role": "tool",
#                 "name": fn_name,
#                 "content": str(tool_result),
#             })

#         # Final Response
#         final = client.chat.completions.create(model=MODEL_ID, messages=messages)
#         return final.choices[0].message.content
    
#     return msg.content


if __name__ == "__main__":
    # print("I want to go from gare delyon to defense")
    #print(run_agent("I want to go from gare delyon to defense"))
    # print(get_station_id("Gare de lyon"))   # Perfect match
    print(get_itinerary("Gare de Lyon", "La Defense"))


