from __future__ import annotations
import json
import re
from typing import Generator

from tools import TOOL_SCHEMAS, TOOL_DISPLAY_NAMES, execute_tool

# Primary path: Claude models that support native tool calling (tried in order)
_TOOL_MODELS = ["claude-3-7-sonnet", "claude-3-5-sonnet", "claude-sonnet-4-6"]
# Fallback synthesis: smaller/faster models are fine for summarisation
_SYNTHESIS_MODELS = ["llama3.3-70b", "llama3.1-70b", "mistral-large2"]
MAX_ITERATIONS = 10

_SYSTEM = """You are a senior Snowflake data platform engineer with deep expertise across the entire data ecosystem. You help teams monitor, diagnose, and optimize their Snowflake environments.

You have access to seven data skills that query live Snowflake Account Usage data:
- query_task_health: Pipeline/task execution (TASK_HISTORY)
- query_warehouse_efficiency: Credit consumption and warehouse cost (WAREHOUSE_METERING_HISTORY)
- query_performance: Query speed, cache hits, data scanned (QUERY_HISTORY)
- query_ingestion_health: Snowpipe and COPY INTO operations (PIPE_USAGE_HISTORY + COPY_HISTORY)
- query_transformation_health: Dynamic table refresh health (DYNAMIC_TABLE_REFRESH_HISTORY)
- query_cost_breakdown: Cross-domain cost attribution (WAREHOUSE_METERING + DATA_TRANSFER)
- query_ecosystem_anomalies: Correlated anomaly signals across all domains

When answering:
1. Call the relevant tools first — never answer without data for operational questions
2. For broad questions ("health check", "what's wrong", "full scan"), call multiple tools
3. Look for cross-domain correlations (e.g., task failures + warehouse credit spikes together)
4. Cite specific values from the returned data in your analysis
5. Lead with the most critical finding, then supporting context
6. End with 2-3 concrete, actionable recommendations"""


# ── Primary path: Cortex COMPLETE with native tool calling ────────────────────

def _complete_with_tools(session, messages: list[dict]) -> tuple[dict, str]:
    options = {"temperature": 0, "tools": TOOL_SCHEMAS, "tool_choice": "auto"}
    for model in _TOOL_MODELS:
        try:
            raw = session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, PARSE_JSON(?), PARSE_JSON(?))::VARCHAR AS r",
                params=[model, json.dumps(messages), json.dumps(options)],
            ).collect()[0]["R"]
            return json.loads(raw), model
        except Exception:
            continue
    raise RuntimeError(f"No tool-calling model available from: {_TOOL_MODELS}")


def _run_tool_calling(question: str, session) -> Generator[dict, None, None]:
    messages: list[dict] = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": question},
    ]
    active_model = _TOOL_MODELS[0]

    for _ in range(MAX_ITERATIONS):
        response, active_model = _complete_with_tools(session, messages)

        content: list[dict] = response.get("content", [])
        stop_reason: str = response.get("stop_reason", "end_turn")
        tool_blocks = [b for b in content if b.get("type") == "tool_use"]

        if stop_reason == "end_turn" or not tool_blocks:
            text = "\n\n".join(
                b.get("text", "") for b in content if b.get("type") == "text"
            ).strip() or "Analysis complete. See the data above."
            yield {"type": "answer", "text": text, "model": active_model}
            return

        messages.append({"role": "assistant", "content": content})

        tool_results: list[dict] = []
        for block in tool_blocks:
            name = block["name"]
            inputs = block.get("input", {})
            use_id = block.get("id", "")
            display = TOOL_DISPLAY_NAMES.get(name, name)

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
                    "tool_use_id": use_id,
                }
                data_text = df.to_string(index=False) if not df.empty else "No data returned."
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": use_id,
                    "content": f"{description}\n\n{data_text}",
                })
            except Exception as exc:
                msg = f"Tool {name} error: {exc}"
                yield {"type": "error", "text": msg}
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": use_id,
                    "content": msg,
                    "is_error": True,
                })

        messages.append({"role": "user", "content": tool_results})

    yield {"type": "answer", "text": "Analysis complete (max iterations reached)."}


# ── Fallback path: instant keyword routing → SQL → synthesis ──────────────────

def _extract_hours(q: str) -> int:
    """Extract time_window_hours from a question string."""
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
    """
    Instant keyword-based tool routing — no LLM call needed.
    Returns list of {"name": str, "inputs": dict}.
    """
    q = question.lower()
    hours = _extract_hours(q)
    tools: list[dict] = []

    # Task / pipeline health
    if re.search(r"task|pipeline|job|fail|error|flak|intermittent|auto.suspend|scheduled", q):
        if re.search(r"slow|durat|long|time", q) and not re.search(r"fail|error", q):
            focus = "duration"
        elif re.search(r"flak|intermittent", q):
            focus = "flaky"
        else:
            focus = "failures"
        tools.append({"name": "query_task_health",
                      "inputs": {"focus": focus, "time_window_hours": hours}})

    # Warehouse / credits / cost
    if re.search(r"warehouse|credit|cost|spend|idle|underutil|expensiv|burn|budget|compute", q):
        if re.search(r"idle|underutil", q):
            focus = "idle"
        elif re.search(r"trend|over.time|daily|histor", q):
            focus = "trend"
        else:
            focus = "most_expensive"
        tools.append({"name": "query_warehouse_efficiency",
                      "inputs": {"focus": focus, "time_window_hours": hours}})

    # Query performance
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
        tools.append({"name": "query_performance",
                      "inputs": {"focus": focus, "time_window_hours": hours}})

    # Data ingestion
    if re.search(r"ingest|snowpipe|\bcopy\b|load(ing|ed|s)?|raw.data|fresh.*data", q):
        if re.search(r"fail|error", q):
            focus = "failed_loads"
        elif re.search(r"\bcopy\b|bulk", q):
            focus = "copy_loads"
        elif re.search(r"pipe|snowpipe", q):
            focus = "pipe_usage"
        else:
            focus = "all"
        tools.append({"name": "query_ingestion_health",
                      "inputs": {"focus": focus, "time_window_hours": hours}})

    # Transformation / dynamic tables
    if re.search(r"dynamic.table|transform|refresh|upstream|stale|lag", q):
        if re.search(r"fail|error", q):
            focus = "failures"
        elif re.search(r"slow|long|durat", q):
            focus = "slowest"
        elif re.search(r"upstream", q):
            focus = "upstream_failures"
        else:
            focus = "all"
        tools.append({"name": "query_transformation_health",
                      "inputs": {"focus": focus, "time_window_hours": hours}})

    # Cross-domain cost breakdown (only when no other tools matched cost already)
    if re.search(r"breakdown|where.*money|total.*cost|overall.*cost|attribution|transfer", q) \
            and not any(t["name"] == "query_warehouse_efficiency" for t in tools):
        tools.append({"name": "query_cost_breakdown",
                      "inputs": {"time_window_hours": hours}})

    # Broad / ecosystem questions — or nothing matched above
    if re.search(r"health.?check|full.?scan|ecosystem|anomal|what.*wrong|everything|overview", q) \
            or not tools:
        tools = [{"name": "query_ecosystem_anomalies",
                  "inputs": {"time_window_hours": hours}}]

    return tools


def _synthesize(question: str, results: list[tuple[str, str, object]], session) -> tuple[str, str]:
    """Summarise all tool results into a concise analyst response."""
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
    # Routing is instant — pure Python, zero LLM calls
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
        answer = "No data could be retrieved. Please check ACCOUNT_USAGE access."
        synth_model = "none"
    elif not has_data:
        answer = "No data found in this time window — your environment looks healthy. Try expanding the range (e.g. 'last 7 days')."
        synth_model = "none"
    else:
        answer, synth_model = _synthesize(question, collected, session)

    model_label = synth_model if synth_model == "none" else f"{synth_model} (fallback)"
    yield {"type": "answer", "text": answer, "model": model_label}


# ── Public entry point ─────────────────────────────────────────────────────────

def run_agent(question: str, session) -> Generator[dict, None, None]:
    """
    Tries native tool calling (Claude) first.
    Falls back to keyword routing + synthesis on any failure.

    Yields event dicts:
      {"type": "tool_call",   "name": str, "display_name": str, "inputs": dict}
      {"type": "tool_result", "name": str, "display_name": str, "df": DataFrame,
                              "description": str, "rows": int, "tool_use_id": str}
      {"type": "answer",      "text": str, "model": str}
      {"type": "error",       "text": str}
    """
    try:
        yield from _run_tool_calling(question, session)
    except Exception as exc:
        yield {"type": "error", "text": f"[Tool calling failed, using fallback] {exc}"}
        yield from _run_fallback(question, session)
