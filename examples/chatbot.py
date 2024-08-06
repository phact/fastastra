from astra_assistants.astra_assistants_manager import AssistantManager
from fasthtml.common import *
from openai import OpenAI
from astra_assistants import patch

# Set up the app, including daisyui and tailwind for the chat component
tlink = Script(src="https://cdn.tailwindcss.com"),
dlink = Link(rel="stylesheet", href="https://cdn.jsdelivr.net/npm/daisyui@4.11.1/dist/full.min.css")
app = FastHTML(hdrs=(tlink, dlink, picolink))

client = patch(OpenAI())
sp = """You are a helpful and concise assistant."""
messages = []

# Chat message component, polling if message is still being generated
def ChatMessage(msg_idx):
    msg = messages[msg_idx]
    text = "..." if msg['content'] == "" else msg['content']
    bubble_class = "chat-bubble-primary" if msg['role']=='user' else 'chat-bubble-secondary'
    chat_class = "chat-end" if msg['role']=='user' else 'chat-start'
    generating = 'generating' in messages[msg_idx] and messages[msg_idx]['generating']
    stream_args = {"hx_trigger":"every 0.1s", "hx_swap":"outerHTML", "hx_get":f"/chat_message/{msg_idx}"}
    return Div(Div(msg['role'], cls="chat-header"),
               Div(text, cls=f"chat-bubble {bubble_class} whitespace-pre-wrap"),
               cls=f"chat {chat_class}", id=f"chat-message-{msg_idx}",
               **stream_args if generating else {})

# Route that gets polled while streaming
@app.get("/chat_message/{msg_idx}")
def get_chat_message(msg_idx:int):
    if msg_idx >= len(messages): return ""
    return ChatMessage(msg_idx)

# The input field for the user message. Also used to clear the
# input field after sending a message via an OOB swap
def ChatInput():
    return Input(type="text", name='msg', id='msg-input',
                 placeholder="Type a message",
                 cls="input input-bordered w-full", hx_swap_oob='true')

# The main screen
@app.route("/")
def get():
    page = Body(H1('Chatbot Demo'),
                Div(*[ChatMessage(msg) for msg in messages],
                    id="chatlist", cls="chat-box h-[73vh] overflow-y-auto"),
                Form(Group(ChatInput(), Button("Send", cls="btn btn-primary")),
                     hx_post="/", hx_target="#chatlist", hx_swap="beforeend",
                     cls="flex space-x-2 mt-2",
                     ), cls="p-4 max-w-lg mx-auto")
    return Title('Chatbot Demo'), page

# Run the chat model in a separate thread
@threaded
def get_response(r, idx):
    for chunk in r:
        messages[idx]["content"] += chunk
    messages[idx]["generating"] = False


manager = AssistantManager(instructions=sp, model="gpt-4o-mini", tools=[])

# Handle the form submission
@app.post("/")
def post(msg:str):
    idx = len(messages)
    messages.append({"role":"user", "content":msg})
    r = manager.stream_thread(content=msg, tool=None) # Send message to chat model (with streaming)
    messages.append({"role":"assistant", "generating":True, "content":""}) # Response initially blank
    get_response(r, idx+1) # Start a new thread to fill in content
    return (ChatMessage(idx), # The user's message
            ChatMessage(idx+1), # The chatbot's response
            ChatInput()) # And clear the input field via an OOB swap


serve()