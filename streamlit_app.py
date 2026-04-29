import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

from agent import run_agent

session = get_active_session()

st.set_page_config(page_title="Snowflake Ecosystem Analyst", layout="wide")

st.markdown(
    """
    <h1 style='color:#29B5E8'>❄️ Snowflake Data Ecosystem Analyst</h1>
    <p style='color:#888; margin-top:-8px'>
        AI agent with live skills across ingestion · transformation · pipelines · compute · cost
    </p>
    <hr style='margin-bottom:12px'>
    """,
    unsafe_allow_html=True,
)

# ── Suggestion buttons ─────────────────────────────────────────────────────────

SUGGESTIONS = [
    ("🌐 Full Ecosystem Scan", "Run a full health check across my entire data ecosystem"),
    ("🔴 Pipeline Failures",   "Which tasks have the highest failure rate?"),
    ("💰 Cost Breakdown",      "Which warehouses are costing the most and why?"),
    ("🐌 Slow Queries",        "Which queries are running the slowest and scanning the most data?"),
    ("📥 Ingestion Health",    "How is my data ingestion performing? Check both Snowpipe and COPY loads."),
    ("🔄 Transform Health",    "Are my dynamic tables refreshing successfully?"),
]

if "messages" not in st.session_state:
    st.session_state.messages = []

st.markdown("**Try asking:**")
cols = st.columns(3)
for i, (label, q) in enumerate(SUGGESTIONS):
    if cols[i % 3].button(label, key=f"sug_{i}", use_container_width=True):
        st.session_state.pending_question = q

# ── Chart helper ───────────────────────────────────────────────────────────────

def _render_chart(df: pd.DataFrame):
    if df.empty:
        return
    numeric = df.select_dtypes(include="number").columns.tolist()
    text = df.select_dtypes(include="object").columns.tolist()
    if not numeric or not text:
        return
    x_col, y_col = text[0], numeric[0]
    color = text[1] if len(text) > 1 else None
    chart = (
        alt.Chart(df)
        .mark_bar(color="#29B5E8")
        .encode(
            x=alt.X(f"{x_col}:N", sort="-y", title=x_col.replace("_", " ").title()),
            y=alt.Y(f"{y_col}:Q", title=y_col.replace("_", " ").title()),
            tooltip=df.columns.tolist(),
            **({"color": alt.Color(f"{color}:N")} if color else {}),
        )
        .properties(height=280)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


# ── Render a stored assistant message from history ─────────────────────────────

def _render_history_message(msg: dict):
    if msg.get("tool_calls"):
        model_badge = f" · `{msg['model']}`" if msg.get("model") else ""
        with st.expander(f"🔧 {len(msg['tool_calls'])} skill(s) invoked{model_badge}", expanded=False):
            for tc in msg["tool_calls"]:
                st.markdown(f"**{tc['display_name']}** — `{tc['name']}`")
                if tc.get("inputs"):
                    st.json(tc["inputs"])

    for tr in msg.get("tool_results", []):
        st.markdown(f"##### 📊 {tr['display_name']}")
        st.caption(tr["description"])
        df = pd.DataFrame(tr["data"])
        if not df.empty:
            _render_chart(df)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No data returned for this skill.")

    if msg.get("answer"):
        st.markdown("---")
        st.markdown("### 🧠 Analysis")
        st.markdown(msg["answer"])

    if msg.get("errors"):
        for err in msg["errors"]:
            st.error(err)


# ── Replay history ─────────────────────────────────────────────────────────────

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        else:
            _render_history_message(msg)


# ── Chat input + live agent run ────────────────────────────────────────────────

question = st.chat_input("Ask about your Snowflake data ecosystem...")

if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    # Accumulators for saving to history
    assistant_record: dict = {
        "role": "assistant",
        "content": question,
        "tool_calls": [],
        "tool_results": [],
        "errors": [],
        "answer": "",
        "model": "",
    }

    with st.chat_message("assistant"):
        # Live tool-call trace container
        trace_container = st.empty()
        trace_lines: list[str] = []

        result_sections: list[dict] = []   # (display_name, description, df) to render after trace
        answer_placeholder = st.empty()

        with st.spinner("Agent is analyzing your ecosystem..."):
            for event in run_agent(question, session):
                etype = event["type"]

                if etype == "tool_call":
                    trace_lines.append(
                        f"🔍 **{event['display_name']}** — querying live data..."
                    )
                    trace_container.markdown("\n\n".join(trace_lines))
                    assistant_record["tool_calls"].append({
                        "name": event["name"],
                        "display_name": event["display_name"],
                        "inputs": event.get("inputs", {}),
                    })

                elif etype == "tool_result":
                    rows = event["rows"]
                    trace_lines[-1] = (
                        f"✅ **{event['display_name']}** — {rows} row{'s' if rows != 1 else ''} returned"
                    )
                    trace_container.markdown("\n\n".join(trace_lines))
                    result_sections.append(event)
                    assistant_record["tool_results"].append({
                        "name": event["name"],
                        "display_name": event["display_name"],
                        "description": event["description"],
                        "data": event["df"].to_dict(orient="records"),
                    })

                elif etype == "error":
                    trace_lines.append(f"⚠️ {event['text']}")
                    trace_container.markdown("\n\n".join(trace_lines))
                    assistant_record["errors"].append(event["text"])

                elif etype == "answer":
                    assistant_record["answer"] = event["text"]
                    assistant_record["model"] = event.get("model", "")

        # Clear spinner; render tool trace as a collapsed expander
        trace_container.empty()
        if assistant_record["tool_calls"]:
            model_badge = f" · `{assistant_record['model']}`" if assistant_record.get("model") else ""
            with st.expander(
                f"🔧 {len(assistant_record['tool_calls'])} skill(s) invoked{model_badge}", expanded=False
            ):
                for line in trace_lines:
                    st.markdown(line)

        # Render each tool result (chart + table)
        for ev in result_sections:
            st.markdown(f"##### 📊 {ev['display_name']}")
            st.caption(ev["description"])
            if not ev["df"].empty:
                _render_chart(ev["df"])
                st.dataframe(ev["df"], use_container_width=True)
            else:
                st.info("No data returned for this skill.")

        # Final analysis
        if assistant_record["answer"]:
            st.markdown("---")
            st.markdown("### 🧠 Analysis")
            st.markdown(assistant_record["answer"])

        for err in assistant_record["errors"]:
            st.error(err)

    st.session_state.messages.append(assistant_record)
