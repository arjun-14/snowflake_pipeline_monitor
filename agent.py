from __future__ import annotations
import json
import re
import requests
import _snowflake  # type: ignore  — Snowflake-internal, not resolvable locally
from typing import Generator
import pandas as pd

from tools import TOOL_DISPLAY_NAMES, execute_tool

MAX_ITERATIONS = 5
_SEMANTIC_MODEL = "@PIPELINE_MONITOR.TASKS.CORTEX_STAGE/semantic_model.yaml"

_SYSTEM = (
    "You are a senior Snowflake data platform engineer. "
    "You help teams monitor, diagnose, and optimize their Snowflake environments. "
    "Always call query_data to fetch live data before answering operational questions. "
    "query_data retrieves factual metrics only — counts, aggregates, and trends from database tables. "
    "NEVER call query_data asking for recommendations, optimizations, or suggestions — "
    "those are your job to synthesize after reviewing the data. "
    "Available data domains you can query: "
    "task/pipeline reliability (failure rates, durations, auto-suspensions), "
    "warehouse credits (total spend, daily trends, idle warehouses), "
    "query performance (slowest queries, data scanned, cache hits, queue times), "
    "Snowpipe ingestion (credits, file volumes, daily trends), "
    "dynamic table refreshes (failure rates, durations), "
    "COPY bulk loads (success rates, rows loaded). "
    "Call query_data once per data domain — ask one focused, single-concept factual question per domain. "
    "Do not combine multiple metrics into one question — ask for the most important metric only. "
    "Never call query_data multiple times for the same domain. "
    "For broad health checks, call once per relevant domain. "
    "Lead with the most critical finding, cite specific numbers from the data, "
    "and end with 2-3 actionable recommendations. "
    "If a question is ambiguous, ask a clarifying question before querying."
)

# One tool: natural language → Cortex Analyst → dynamic SQL from semantic model
_OAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_data",
            "description": (
                "Query live Snowflake Account Usage data by asking a focused natural language question. "
                "Internally uses Cortex Analyst with a semantic model covering: task/pipeline execution, "
                "warehouse credit consumption, query performance, Snowpipe and COPY ingestion, "
                "dynamic table refresh health, and cost attribution. "
                "Ask one focused question per call. For broad analysis, call multiple times."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": (
                            "A specific, focused question about Snowflake data. "
                            "Examples: 'Which tasks have the highest failure rate in the last 24 hours?', "
                            "'Which warehouses consumed the most credits this week?', "
                            "'What are the slowest queries by execution time?'"
                        ),
                    }
                },
                "required": ["question"],
            },
        },
    }
]


_DOMAIN_LABELS = [
    (re.compile(r"task|pipeline|job|fail|suspend|scheduled", re.I), "Pipeline Health"),
    (re.compile(r"warehouse|credit|cost|spend|idle|compute", re.I), "Warehouse Credits"),
    (re.compile(r"quer(y|ies)|slow|scan|cache|execution", re.I), "Query Performance"),
    (re.compile(r"pipe|snowpipe|ingest", re.I), "Snowpipe Ingestion"),
    (re.compile(r"dynamic.table|refresh|upstream|transform", re.I), "Transform Health"),
    (re.compile(r"copy|load|bulk|row", re.I), "Load Health"),
]


def _skill_label(question: str) -> str:
    for pattern, label in _DOMAIN_LABELS:
        if pattern.search(question):
            return label
    return "Cortex Analyst"


# ── Cortex Analyst: natural language → SQL → DataFrame ────────────────────────

def _cortex_analyst(question: str) -> tuple[str, str]:
    """Calls Cortex Analyst. Returns (sql, description)."""
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        {
            "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
            "semantic_model_file": _SEMANTIC_MODEL,
        },
        None,
        30000,
    )

    if resp["status"] >= 400:
        raise RuntimeError(f"Cortex Analyst {resp['status']}: {resp['content']}")

    body = json.loads(resp["content"]) if isinstance(resp["content"], str) else resp["content"]
    blocks = body.get("message", {}).get("content", [])

    sql, description = "", question
    for block in blocks:
        if block.get("type") == "sql":
            sql = block.get("statement", "")
        elif block.get("type") == "text":
            description = block.get("text", question)

    if not sql:
        raise RuntimeError("Cortex Analyst did not return SQL for this question.")

    return sql, description


def _execute_analyst_query(question: str, session) -> tuple[pd.DataFrame, str, str]:
    sql, description = _cortex_analyst(question)
    df = session.sql(sql).to_pandas()
    return df, description, sql


# ── Cortex Chat Completions primary path ──────────────────────────────────────

def _build_messages(question: str, history: list[dict]) -> list[dict]:
    messages: list[dict] = [{"role": "system", "content": _SYSTEM}]
    for msg in history:
        if msg["role"] == "user":
            messages.append({"role": "user", "content": msg["content"]})
        elif msg["role"] == "assistant" and msg.get("answer"):
            messages.append({"role": "assistant", "content": msg["answer"]})
    messages.append({"role": "user", "content": question})
    return messages


def _run_chat_completions(question: str, history: list[dict], session) -> Generator[dict, None, None]:
    messages = _build_messages(question, history)

    for _ in range(MAX_ITERATIONS):
        resp = _snowflake.send_snow_api_request(
            "POST",
            "/api/v2/cortex/v1/chat/completions",
            {},
            {},
            {"model": "claude-4-sonnet", "messages": messages, "tools": _OAI_TOOLS, "tool_choice": "auto"},
            None,
            60000,
        )

        if resp["status"] >= 400:
            raise RuntimeError(f"Chat Completions {resp['status']}: {resp['content']}")

        data = json.loads(resp["content"]) if isinstance(resp["content"], str) else resp["content"]
        message = data["choices"][0]["message"]
        tool_calls = message.get("tool_calls") or []

        if not tool_calls:
            yield {
                "type": "answer",
                "text": (message.get("content") or "").strip() or "Analysis complete.",
                "model": data.get("model", "claude-4-sonnet"),
            }
            return

        messages.append({"role": "assistant", "content": message.get("content") or "", "tool_calls": tool_calls})

        tool_results = []
        for tc in tool_calls:
            try:
                inputs = json.loads(tc["function"].get("arguments") or "{}")
            except Exception:
                inputs = {}
            sub_question = inputs.get("question", question)
            use_id = tc.get("id", "")

            skill = _skill_label(sub_question)
            yield {
                "type": "tool_call",
                "name": "query_data",
                "display_name": skill,
                "inputs": {"question": sub_question},
            }

            try:
                df, _, sql = _execute_analyst_query(sub_question, session)
                yield {
                    "type": "tool_result",
                    "name": "query_data",
                    "display_name": skill,
                    "df": df,
                    "description": sub_question,
                    "rows": len(df),
                    "tool_use_id": use_id,
                }
                data_text = df.to_string(index=False) if not df.empty else "No data returned."
                tool_results.append({
                    "role": "tool",
                    "tool_call_id": use_id,
                    "content": f"Question: {sub_question}\nSQL: {sql}\n\nResults:\n{data_text}",
                })
            except Exception as exc:
                msg = f"query_data error: {exc}"
                yield {"type": "error", "text": msg}
                tool_results.append({"role": "tool", "tool_call_id": use_id, "content": msg})

        messages.extend(tool_results)

    yield {"type": "answer", "text": "Analysis complete (max iterations reached).", "model": "claude-4-sonnet"}
# ── Fallback path: keyword routing + hardcoded SQL + llama synthesis ───────────

def _extract_hours(q: str) -> int:
    m = re.search(r"last\s+(\d+)\s+hour", q)
    if m:
        return int(m.group(1))
    m = re.search(r"last\s+(\d+)\s+day", q)
    if m:
        return int(m.group(1)) * 24
    if re.search(r"last\s+week|last\s+7\s+day", q):
        return 168
    if re.search(r"last\s+month|last\s+30\s+day", q):
        return 720
    if re.search(r"last\s+2\s+day|yesterday", q):
        return 48
    return 24


def _route(question: str) -> list[dict]:
    q = question.lower()
    hours = _extract_hours(q)
    tools: list[dict] = []

    if re.search(r"task|pipeline|job|fail|error|flak|intermittent|auto.suspend|scheduled", q):
        if re.search(r"slow|durat|long|time", q) and not re.search(r"fail|error", q):
            focus = "duration"
        elif re.search(r"flak|intermittent", q):
            focus = "flaky"
        else:
            focus = "failures"
        tools.append({"name": "query_task_health", "inputs": {"focus": focus, "time_window_hours": hours}})

    if re.search(r"warehouse|credit|cost|spend|idle|underutil|expensiv|burn|budget|compute", q):
        if re.search(r"idle|underutil", q):
            focus = "idle"
        elif re.search(r"trend|over.time|daily|histor", q):
            focus = "trend"
        else:
            focus = "most_expensive"
        tools.append({"name": "query_warehouse_efficiency", "inputs": {"focus": focus, "time_window_hours": hours}})

    if re.search(r"\bquer(y|ies)\b|cache|scan|perform|execut", q):
        if re.search(r"slow|slowest|long|longest", q):
            focus = "slowest"
        elif re.search(r"scan|gb|bytes|data.*scan", q):
            focus = "most_data_scanned"
        elif re.search(r"fail|error", q) and "task" not in q:
            focus = "failed"
        elif re.search(r"cache", q):
            focus = "cache_misses"
        else:
            focus = "all"
        tools.append({"name": "query_performance", "inputs": {"focus": focus, "time_window_hours": hours}})

    if re.search(r"ingest|snowpipe|\bcopy\b|load(ing|ed|s)?|raw.data|fresh.*data", q):
        if re.search(r"fail|error", q):
            focus = "failed_loads"
        elif re.search(r"\bcopy\b|bulk", q):
            focus = "copy_loads"
        elif re.search(r"pipe|snowpipe", q):
            focus = "pipe_usage"
        else:
            focus = "all"
        tools.append({"name": "query_ingestion_health", "inputs": {"focus": focus, "time_window_hours": hours}})

    if re.search(r"dynamic.table|transform|refresh|upstream|stale|lag", q):
        if re.search(r"fail|error", q):
            focus = "failures"
        elif re.search(r"slow|long|durat", q):
            focus = "slowest"
        elif re.search(r"upstream", q):
            focus = "upstream_failures"
        else:
            focus = "all"
        tools.append({"name": "query_transformation_health", "inputs": {"focus": focus, "time_window_hours": hours}})

    if re.search(r"breakdown|where.*money|total.*cost|overall.*cost|attribution|transfer", q) \
            and not any(t["name"] == "query_warehouse_efficiency" for t in tools):
        tools.append({"name": "query_cost_breakdown", "inputs": {"time_window_hours": hours}})

    if re.search(r"health.?check|full.?scan|ecosystem|anomal|what.*wrong|everything|overview", q) \
            or not tools:
        tools = [{"name": "query_ecosystem_anomalies", "inputs": {"time_window_hours": hours}}]

    return tools


def _synthesize(question: str, results: list[tuple[str, str, object]], session) -> tuple[str, str]:
    _SYNTHESIS_MODELS = ["llama3.3-70b", "llama3.1-70b", "mistral-large2"]
    data_sections = "\n\n".join(
        f"### {display}\n{desc}\n\n"
        + (df.to_string(index=False) if not df.empty else "No data.")  # type: ignore[union-attr]
        for display, desc, df in results
    )
    prompt = (
        f"You are a senior Snowflake data platform engineer.\n\n"
        f"A user asked: \"{question}\"\n\n"
        f"Here is live Account Usage data:\n\n{data_sections}\n\n"
        "Write a concise analysis (3-5 sentences). Lead with the most critical finding, "
        "cite specific numbers, and end with 2-3 actionable recommendations."
    )
    for model in _SYNTHESIS_MODELS:
        try:
            raw = session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS r",
                params=[model, prompt],
            ).collect()[0]["R"]
            return raw, model
        except Exception:
            continue
    raise RuntimeError(f"No synthesis model available from: {_SYNTHESIS_MODELS}")


def _run_fallback(question: str, session) -> Generator[dict, None, None]:
    tool_calls = _route(question)
    collected: list[tuple[str, str, object]] = []

    for tc in tool_calls:
        name, inputs = tc["name"], tc["inputs"]
        display = TOOL_DISPLAY_NAMES[name]
        yield {"type": "tool_call", "name": name, "display_name": display, "inputs": inputs}
        try:
            df, description = execute_tool(name, inputs, session)
            yield {
                "type": "tool_result",
                "name": name,
                "display_name": display,
                "df": df,
                "description": description,
                "rows": len(df),
                "tool_use_id": "",
            }
            collected.append((display, description, df))
        except Exception as exc:
            yield {"type": "error", "text": f"Tool {name} error: {exc}"}

    has_data = any(not df.empty for _, _, df in collected)

    if not collected:
        yield {"type": "answer", "text": "No data could be retrieved. Please check ACCOUNT_USAGE access.", "model": "none"}
    elif not has_data:
        yield {"type": "answer", "text": "No data found in this time window — your environment looks healthy.", "model": "none"}
    else:
        answer, synth_model = _synthesize(question, collected, session)
        yield {"type": "answer", "text": answer, "model": synth_model}


# ── Public entry point ─────────────────────────────────────────────────────────

def run_agent(question: str, history: list[dict], session) -> Generator[dict, None, None]:
    """
    Primary: Claude (claude-4-sonnet) via Cortex Chat Completions drives the agentic loop.
             query_data() routes through Cortex Analyst → dynamic SQL from semantic model.
    Fallback: keyword routing + hardcoded SQL (tools.py) + llama synthesis.

    Yields event dicts:
      {"type": "tool_call",   "name": str, "display_name": str, "inputs": dict}
      {"type": "tool_result", "name": str, "display_name": str, "df": DataFrame,
                              "description": str, "rows": int, "tool_use_id": str}
      {"type": "answer",      "text": str, "model": str}
      {"type": "error",       "text": str}
    """
    try:
        yield from _run_chat_completions(question, history, session)
    except Exception as exc:
        yield {"type": "error", "text": f"[Primary failed, using fallback] {exc}"}
        yield from _run_fallback(question, session)
