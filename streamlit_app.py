import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

from agent import run_agent

session = get_active_session()

st.set_page_config(page_title="Snowflake Ecosystem Analyst", layout="wide")

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        """
        <h2 style='color:#29B5E8; margin-bottom:2px'>Snowflake Analyst</h2>
        <p style='color:#888; font-size:13px; margin-top:0'>
            Live insights across pipelines, compute, queries, and ingestion
        </p>
        <hr style='margin:12px 0'>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<p style='font-size:13px; color:#aaa; margin-bottom:8px'>Quick questions</p>", unsafe_allow_html=True)

    SUGGESTIONS = [
        ("Pipeline Failures",    "Which tasks have the highest failure rate in the last 7 days?"),
        ("Warehouse Costs",      "Which warehouses consumed the most credits in the last 7 days?"),
        ("Slow Queries",         "Which queries ran the slowest in the last 7 days?"),
        ("Ingestion Health",     "How many rows were loaded via COPY commands in the last 7 days?"),
        ("Slowest Pipelines",    "Which tasks are taking the longest to run on average in the last 7 days?"),
        ("Idle Warehouses",      "Which warehouses are underutilized or idle?"),
    ]

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for label, q in SUGGESTIONS:
        if st.button(label, key=f"sug_{label}", use_container_width=True):
            st.session_state.pending_question = q

    st.markdown("<hr style='margin:16px 0'>", unsafe_allow_html=True)
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ── Main header ────────────────────────────────────────────────────────────────

st.markdown(
    """
    <h1 style='color:#29B5E8; margin-bottom:2px'>Snowflake Data Ecosystem Analyst</h1>
    <p style='color:#888; margin-top:0; margin-bottom:20px; font-size:14px'>
        Real-time operational intelligence across your Snowflake data platform, 
        monitor pipeline reliability, warehouse costs, query performance, and ingestion health
        using live Account Usage data.
    </p>
    """,
    unsafe_allow_html=True,
)


# ── Chart helper ───────────────────────────────────────────────────────────────

def _fix_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(5)
        if len(sample) == 0:
            continue
        if all(isinstance(v, str) and v.replace(".", "").replace("-", "").isdigit() for v in sample):
            try:
                numeric_vals = pd.to_numeric(df[col])
                if numeric_vals.max() > 1e12:
                    df[col] = pd.to_datetime(numeric_vals, unit="ms").dt.strftime("%Y-%m-%d %H:%M")
                elif numeric_vals.max() > 1e9:
                    df[col] = pd.to_datetime(numeric_vals, unit="s").dt.strftime("%Y-%m-%d %H:%M")
                else:
                    df[col] = numeric_vals
            except (ValueError, TypeError):
                pass
    return df
    
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
        .properties(height=260)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


# ── Render a stored assistant message from history ─────────────────────────────

def _render_history_message(msg: dict):
    if msg.get("tool_calls"):
        n = len(msg["tool_calls"])
        with st.expander(f"{n} {'query' if n == 1 else 'queries'}", expanded=False):
            for line in msg.get("trace_lines", []):
                st.markdown(line)

    for tr in msg.get("tool_results", []):
        st.caption(tr["display_name"])
        df = _fix_timestamps(pd.DataFrame(tr["data"]))
        if not df.empty:
            _render_chart(df)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No data returned.")

    if msg.get("answer"):
        st.divider()
        st.markdown(msg["answer"])

    if msg.get("errors") and not msg.get("answer"):
        for err in msg["errors"]:
            st.error(err)


# ── Replay history ─────────────────────────────────────────────────────────────

for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(
            f"<div style='background:#1C2B3A; border-left:3px solid #29B5E8; "
            f"padding:10px 14px; border-radius:4px; margin:8px 0'>"
            f"<span style='color:#888; font-size:11px; text-transform:uppercase; letter-spacing:1px'>You</span>"
            f"<br><span style='color:#eee'>{msg['content']}</span></div>",
            unsafe_allow_html=True,
        )
    else:
        with st.container():
            _render_history_message(msg)


# ── Chat input + live agent run ────────────────────────────────────────────────

with st.form("chat_form", clear_on_submit=True):
    cols = st.columns([9, 1])
    with cols[0]:
        q_input = st.text_input(
            "", placeholder="Ask about your Snowflake environment...",
            label_visibility="collapsed",
        )
    with cols[1]:
        submitted = st.form_submit_button("Send", use_container_width=True)

question = q_input.strip() if submitted and q_input.strip() else None

if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    st.markdown(
        f"<div style='background:#1C2B3A; border-left:3px solid #29B5E8; "
        f"padding:10px 14px; border-radius:4px; margin:8px 0'>"
        f"<span style='color:#888; font-size:11px; text-transform:uppercase; letter-spacing:1px'>You</span>"
        f"<br><span style='color:#eee'>{question}</span></div>",
        unsafe_allow_html=True,
    )

    assistant_record: dict = {
        "role": "assistant",
        "content": question,
        "tool_calls": [],
        "tool_results": [],
        "trace_lines": [],
        "errors": [],
        "answer": "",
        "model": "",
    }

    with st.container():
        trace_container = st.empty()
        result_sections: list[dict] = []

        with st.spinner("Fetching live data..."):
            for event in run_agent(question, st.session_state.messages[:-1], session):
                etype = event["type"]

                if etype == "tool_call":
                    sub_q = event.get("inputs", {}).get("question", "")
                    label = f": {sub_q}" if sub_q else ""
                    assistant_record["trace_lines"].append(f"{event['display_name']}{label}")
                    assistant_record["tool_calls"].append({
                        "name": event["name"],
                        "display_name": event["display_name"],
                        "inputs": event.get("inputs", {}),
                    })

                elif etype == "tool_result":
                    rows = event["rows"]
                    last = assistant_record["trace_lines"][-1]
                    assistant_record["trace_lines"][-1] = last + f" ({rows} {'row' if rows == 1 else 'rows'})"
                    result_sections.append(event)
                    assistant_record["tool_results"].append({
                        "name": event["name"],
                        "display_name": event["display_name"],
                        "description": event["description"],
                        "data": event["df"].to_dict(orient="records"),
                    })

                elif etype == "error":
                    assistant_record["trace_lines"].append(event["text"])
                    assistant_record["errors"].append(event["text"])

                elif etype == "answer":
                    assistant_record["answer"] = event["text"]
                    assistant_record["model"] = event.get("model", "")

        trace_container.empty()

        if assistant_record["tool_calls"]:
            n = len(assistant_record["tool_calls"])
            with st.expander(f"{n} {'query' if n == 1 else 'queries'}", expanded=False):
                for line in assistant_record["trace_lines"]:
                    st.markdown(line)

        for ev in result_sections:
            st.caption(ev["display_name"])
            if not ev["df"].empty:
                df = _fix_timestamps(ev["df"])
                _render_chart(df)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No data returned.")

        if assistant_record["answer"]:
            st.divider()
            st.markdown(assistant_record["answer"])

        if assistant_record["errors"] and not assistant_record["answer"]:
            for err in assistant_record["errors"]:
                st.error(err)

    st.session_state.messages.append(assistant_record)
