import os
import re
import unicodedata
import gradio as gr

from tools import get_station_id, get_itinerary, get_disruption_context


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _parse_itinerary(q: str):
    raw = q.strip()

    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+)$", raw, flags=re.I)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    for sep in [" -> ", " => ", " to "]:
        if sep in raw:
            a, b = raw.split(sep, 1)
            a, b = a.strip(), b.strip()
            if a and b:
                return a, b
    return None, None


def _parse_station_lookup(q: str):
    m = re.search(r"(find\s+station|station)\s*:\s*(.+)$", q, flags=re.I)
    if m:
        return m.group(2).strip()
    return None


def _is_disruption(qn: str) -> bool:
    return any(w in qn for w in ["disruption", "incident", "perturb", "probleme", "panne", "retard", "traffic"])


def _is_station_lookup(qn: str) -> bool:
    return ("find station" in qn) or qn.startswith("station:") or ("station :" in qn)


def route(message: str) -> str:
    q = (message or "").strip()
    if not q:
        return "Write a request (itinerary, disruptions, station lookup)."

    qn = _norm(q)

    # 1) station lookup
    if _is_station_lookup(qn):
        name = _parse_station_lookup(q) or q
        return f"### Station lookup\n{get_station_id(name)}"

    # 2) disruptions
    if _is_disruption(qn):
        return get_disruption_context(q)

    # 3) itinerary
    a, b = _parse_itinerary(q)
    if a and b:
        return get_itinerary(a, b)

    # help
    return (
        "I can handle:\n"
        "- **Itinerary**: `Gare de Lyon -> Châtelet` or `from Gare de Lyon to Châtelet`\n"
        "- **Disruptions**: `Any disruption on Metro 14?` / `Any disruption on RER A?` / `Any disruption on bus?`\n"
        "- **Station lookup**: `Find station: Nation`\n"
        "\nSupported: Metro + Bus + RER. Not supported: Tram / TER."
    )


TITLE = "🚇🚌🚆 Île-de-France Metro + Bus + RER Agent"
DESC = "Supported: Metro + Bus + RER. Not supported: Tram / TER."
EXAMPLES = [
    "Gare de Lyon -> Châtelet",
    "Any disruption on Metro 14?",
    "Any disruption on RER A?",
    "Any disruption on bus?",
    "Find station: Nation",
    "Find station: Boulogne - Billancourt",
]


with gr.Blocks() as demo:
    gr.Markdown(f"# {TITLE}\n\n{DESC}\n\n**Try:**")
    gr.Markdown("\n".join([f"- {e}" for e in EXAMPLES[:4]]))

    chatbot = gr.Chatbot(height=560)  # Gradio 6 expects messages dicts internally
    state = gr.State([])  # we store a list of {"role","content"}

    with gr.Row():
        msg = gr.Textbox(placeholder="Type here… (e.g., Gare de Lyon -> Châtelet)", scale=8)
        send = gr.Button("Send", scale=1)
        clear = gr.Button("Clear", scale=1)

    gr.Examples(examples=EXAMPLES, inputs=msg)

    def respond(user_message, history):
        history = history or []
        # enforce messages format
        if not isinstance(history, list):
            history = []
        history.append({"role": "user", "content": user_message})

        try:
            answer = route(user_message)
        except Exception as e:
            answer = f"Internal error: {str(e)}"

        history.append({"role": "assistant", "content": answer})
        return "", history, history

    def do_clear():
        return [], []

    send.click(respond, inputs=[msg, state], outputs=[msg, chatbot, state])
    msg.submit(respond, inputs=[msg, state], outputs=[msg, chatbot, state])
    clear.click(do_clear, outputs=[chatbot, state])


if __name__ == "__main__":
    demo.queue()
    demo.launch(
        server_name=os.getenv("GRADIO_SERVER_NAME", "0.0.0.0"),
        server_port=int(os.getenv("PORT", "7860")),
    )
