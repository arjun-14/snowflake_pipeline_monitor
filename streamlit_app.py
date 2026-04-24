import streamlit as st
import requests
import json
import pandas as pd
import altair as alt
import _snowflake
from snowflake.snowpark.context import get_active_session


# Setup
session = get_active_session()
STAGE = "@PIPELINE_MONITOR.TASKS.CORTEX_STAGE"
SEMANTIC_MODEL = f"{STAGE}/semantic_model.yaml"

st.set_page_config(page_title="Pipeline Health Monitor", layout="wide")

# Header
st.markdown("""
    <h1 style='color:#29B5E8'>❄️ Pipeline Health Monitor</h1>
    <p style='color:#666'>Ask questions about your Snowflake pipelines in plain English</p>
    <hr>
""", unsafe_allow_html=True)

# Suggested questions
SUGGESTIONS = [
    "Which tasks have the highest failure rate?",
    "How many credits has each warehouse consumed?",
    "Which pipelines take the longest to run?",
    "Which warehouses are idle or underutilized?",
    "Which pipelines are intermittently failing?",
    "Which queries are scanning the most data?",
]

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

def query_cortex_analyst(question: str) -> dict:
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        "semantic_model_file": SEMANTIC_MODEL,
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    return json.loads(resp["content"])

def extract_sql(response: dict) -> str | None:
    for item in response.get("message", {}).get("content", []):
        if item.get("type") == "sql":
            return item.get("statement")
    return None

def extract_text(response: dict) -> str | None:
    for item in response.get("message", {}).get("content", []):
        if item.get("type") == "text":
            return item.get("text")
    return None

def generate_summary(question: str, data: str) -> str:
    """Use Cortex COMPLETE to generate an executive summary."""
    prompt = f"""You are a data engineering operations expert analyzing Snowflake pipeline health.
    
A user asked: "{question}"

Here is the data returned:
{data}

Write a concise 2-3 sentence executive summary of what this data reveals. 
Focus on actionable insights and flag any concerning patterns.
Be direct and specific, referencing actual values from the data."""

    result = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', ?) AS summary",
        params=[prompt]
    ).collect()
    return result[0]["SUMMARY"]

def render_chart(df: pd.DataFrame, question: str):
    """Render the most appropriate chart based on data shape."""
    cols = df.columns.tolist()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    text_cols = df.select_dtypes(include="object").columns.tolist()

    if not numeric_cols:
        return

    # Bar chart: one text dimension and one or two numeric measures
    if len(text_cols) >= 1 and len(numeric_cols) >= 1:
        x_col = text_cols[0]
        y_col = numeric_cols[0]
        color_col = text_cols[1] if len(text_cols) > 1 else None

        chart = alt.Chart(df).mark_bar(color="#29B5E8").encode(
            x=alt.X(f"{x_col}:N", sort="-y", title=x_col.replace("_", " ").title()),
            y=alt.Y(f"{y_col}:Q", title=y_col.replace("_", " ").title()),
            tooltip=cols,
            **({"color": alt.Color(f"{color_col}:N")} if color_col else {})
        ).properties(height=350).interactive()

        st.altair_chart(chart, use_container_width=True)

# Suggested questions UI
st.markdown("**💡 Try asking:**")
cols = st.columns(3)
for i, suggestion in enumerate(SUGGESTIONS):
    if cols[i % 3].button(suggestion, key=f"suggestion_{i}"):
        st.session_state.pending_question = suggestion

# Chat input
question = st.chat_input("Ask about your pipeline health...")

# Handle suggestion clicks
if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

# Render chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["role"] == "user":
            st.write(message["content"])
        else:
            st.markdown(f"### 🧠 Executive Summary")
            st.info(message["summary"])
            if message.get("chart_data") is not None:
                render_chart(pd.DataFrame(message["chart_data"]), message["content"])
            st.markdown("### 📊 Data")
            st.dataframe(pd.DataFrame(message["chart_data"]), use_container_width=True)
            with st.expander("🔍 Generated SQL"):
                st.code(message["sql"], language="sql")

# Process new question
if question:
    st.session_state.messages.append({"role": "user", "content": question})

    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing your pipelines..."):
            try:
                # Query Cortex Analyst
                response = query_cortex_analyst(question)
                sql = extract_sql(response)
                interpretation = extract_text(response)

                if not sql:
                    st.warning(interpretation or "I could not generate a query for that question. Try rephrasing.")
                else:
                    # Execute SQL
                    df = session.sql(sql).to_pandas()

                    # Generate executive summary
                    summary = generate_summary(question, df.to_string(index=False))

                    # Render results
                    st.markdown("### 🧠 Executive Summary")
                    st.info(summary)

                    render_chart(df, question)

                    st.markdown("### 📊 Data")
                    st.dataframe(df, use_container_width=True)

                    with st.expander("🔍 Generated SQL"):
                        st.code(sql, language="sql")

                    # Save to history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": question,
                        "summary": summary,
                        "sql": sql,
                        "chart_data": df.to_dict(orient="records")
                    })

            except Exception as e:
                st.error(f"Something went wrong: {str(e)}")