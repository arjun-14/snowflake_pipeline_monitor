# Pipeline Health Monitor

![Architecture](Gemini_Generated_Image_apsi9japsi9japsi.png)

A Streamlit app that lets you ask plain-English questions about your Snowflake pipeline health. It uses **Snowflake Cortex Analyst** to translate natural language into SQL, executes the query against `SNOWFLAKE.ACCOUNT_USAGE`, and uses **Cortex COMPLETE** (Mistral Large) to generate an executive summary of the results.

## What it does

You type a question like *"Which tasks have the highest failure rate?"* and the app:

1. Sends the question to Cortex Analyst, which generates SQL using the semantic model
2. Executes the SQL against Snowflake Account Usage views
3. Renders a bar chart (where applicable) and a data table
4. Uses Cortex COMPLETE to write a 2–3 sentence actionable summary
5. Shows the generated SQL in an expandable section

Chat history is preserved within the session so you can ask follow-up questions.

## Data sources

The semantic model (`semantic_model.yaml`) exposes three Account Usage views, all filtered to the **last 30 days** by default:

| Table | Source view | What it tracks |
|---|---|---|
| `task_history` | `SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY` | Task/pipeline execution outcomes, durations, failure reasons |
| `warehouse_metering` | `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` | Hourly credit consumption by warehouse |
| `query_history` | `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | Query performance, bytes scanned, execution status |

## Example questions

- Which tasks have the highest failure rate?
- How many credits has each warehouse consumed?
- Which pipelines take the longest to run?
- Which warehouses are idle or underutilized?
- Which pipelines are intermittently failing?
- Which queries are scanning the most data?

## Setup

### Prerequisites

- Snowflake account with access to `SNOWFLAKE.ACCOUNT_USAGE`
- Snowflake Cortex Analyst enabled
- Snowpark-enabled Streamlit (runs natively inside Snowflake)

### Deployment

1. Upload `semantic_model.yaml` to the stage referenced in the app:
   ```
   @PIPELINE_MONITOR.TASKS.CORTEX_STAGE
   ```

2. Deploy `streamlit_app.py` as a Streamlit in Snowflake app in the `PIPELINE_MONITOR.TASKS` schema.

3. The app picks up the active Snowpark session automatically — no credentials are needed in the code.

## Project structure

```
streamlit_app.py       # Streamlit UI and Cortex Analyst integration
semantic_model.yaml    # Natural language → SQL mapping for Cortex Analyst
```
