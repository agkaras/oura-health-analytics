import streamlit as st
import requests
from datetime import datetime

N8N_WEBHOOK_URL = "http://localhost:5678/webhook/oura/chat"
N8N_RESET_URL = "http://localhost:5678/webhook/oura/reset"

st.set_page_config(page_title="Oura Health Assistant", page_icon="💍", layout="centered")

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 💍 Oura Ring AI Assistant")
    st.markdown("Ask questions about your Oura health data and track health patterns")
    st.divider()
    if st.button("🗑️ Clear conversation", use_container_width=True, type="secondary"):
        try:
            requests.post(N8N_RESET_URL, timeout=5)
        except Exception:
            pass
        st.session_state.messages = []
        st.rerun()

# ── Init state ────────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []

# ── Header ───────────────────────────────────────────────────────────────────
st.title("Oura Health Assistant 💍")
st.caption("Ask about your Oura Ring data — sleep, activity, HRV, and more.")

# ── Chat history ──────────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if "time" in msg:
            st.caption(msg["time"])

# ── Input ─────────────────────────────────────────────────────────────────────
prompt = st.chat_input("Ask about your Oura data...")

if prompt:
    now = datetime.now().strftime("%H:%M")
    st.session_state.messages.append({"role": "user", "content": prompt, "time": now})
    with st.chat_message("user"):
        st.write(prompt)
        st.caption(now)

    with st.chat_message("assistant"):
        with st.spinner("Analysing your data..."):
            try:
                response = requests.post(
                    N8N_WEBHOOK_URL,
                    json={"question": prompt},
                    timeout=60,
                )
                response.raise_for_status()
                reply = response.json().get("reply", "No response received.")
            except requests.exceptions.ConnectionError:
                reply = "⚠️ Could not connect to n8n. Make sure the server is running."
            except requests.exceptions.Timeout:
                reply = "⏱️ The request timed out. Please try again."
            except Exception as e:
                reply = f"❌ An error occurred: {str(e)}"

        now_reply = datetime.now().strftime("%H:%M")
        st.write(reply)
        st.caption(now_reply)

    st.session_state.messages.append({"role": "assistant", "content": reply, "time": now_reply})
