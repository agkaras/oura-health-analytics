# Oura Ring Personal Health Analytics

> **v1.0 — initial release.** A working prototype built as a personal learning project and portfolio piece. Architecture and code quality are actively being improved.

An end-to-end personal health analytics platform that ingests Oura Ring biometric data into a PostgreSQL database and exposes it through an AI-powered conversational interface. The system is built as a workflow-orchestrated pipeline using n8n, handling data ingestion, scheduling, and AI routing, with PostgreSQL for analytical queries and Streamlit as the user interface.

Its core capability is **pattern discovery based on custom user-defined tag**s (e.g., symptoms, lifestyle factors), with built-in temporal analysis of physiological changes before, during, and after tagged events. This allows users to uncover relationships between various personal events and signals such as HRV, sleep, and recovery — a level of insight not available in the native Oura app. Summary and trend conversational analyses are possible as well.

Ask questions in natural language and receive structured, evidence-based insights into your sleep, recovery, activity, stress, menstrual cycle, and personalized health recommendations. 

---

## What it does

**Example queries the system can answer:**

- *"What happens to my resting heart rate during and after having a cold?"*
- *"What tags correlate with low HRV?"*
- _"Do I show early physiological signals before tagged stress events?"_
- *"Show me my readiness trend over the last 30 days"*
- *"What is my migraine risk today based on my previous data?"*
- *"Give me a health overview of the past week"*


The AI response always includes today's actual metrics — HRV in milliseconds, resting heart rate in bpm, cycle day and phase, stress minutes — pulled from the correct sensor columns rather than Oura's readiness contributor scores.

---

## Architecture

```
Oura Ring API v2
       │
       ▼
  n8n (self-hosted, Docker)
       │
       ├── 1. Historical backfill      ← run once to load all history
       │         └── upserts 8 data types + rebuilds oura_cycles
       │
       ├── 2. Daily update             ← scheduled, runs automatically
       │         └── rolling 7-day window + refreshes oura_cycles
       │
       └── 3. AI chat workflow         ← called by Streamlit on each message
               ├── AI classifier       → structured routing params
               ├── Normalizer (JS)     → validation & edge-case handling
               ├── Switch node         → 5 analysis branches
               │     ├── event_pattern    (tag-based, with day offsets)
               │     ├── inverse_lookup   (what correlates with low X?)
               │     ├── trend            (rolling 7/30/90-day averages)
               │     ├── summary          (period snapshot)
               │     └── cycle_analysis   (menstrual cycle biomarkers)
               ├── PostgreSQL queries  → pre-aggregated results
               ├── Context Builder     → structured JSON for AI
               └── AI Agent            → natural language analysis

Streamlit (chat.py)
       │
       └── HTTP → n8n webhook → AI workflow → response
```

---

## Key Technical Decisions

**Aggregation over iteration** — all SQL queries return pre-aggregated results via `UNION ALL` with a `result_type` discriminator column. This reduces output from tens of thousands of rows to ~30–50 rows per query, making LLM analysis reliable and cost-efficient.

**Correct sensor columns** — Oura API v2 exposes both raw sensor values (`average_hrv` in ms, `lowest_heart_rate` in bpm from `oura_sleep`) and readiness contributor scores (`hrv_balance`, `resting_heart_rate` as 1–100 from `oura_daily_readiness`). The pipeline explicitly separates these to prevent the AI from misreporting scores as physiological values.

**Cycle-aware analysis** — a custom `oura_cycles` table is derived from period tags, computing `cycle_day`, `cycle_phase`, and `temperature_phase` per calendar day using a LATERAL join approach. Every analysis branch joins this table so all health insights include hormonal context.

**Workout integration** — `oura_workouts` data is joined into all analysis branches, enabling the AI to detect patterns such as exercise as a migraine trigger or its effect on next-day HRV recovery.

**Conversation memory** — the AI chat workflow maintains a rolling 10-turn conversation history via n8n static data, enabling follow-up questions without repeating context.

---

## Stack

| Layer | Technology |
|---|---|
| Data source | Oura Ring API v2 |
| Database | PostgreSQL |
| Orchestration | n8n (self-hosted, Docker) |
| Classification LLM | Google Gemini (structured JSON output) |
| Analysis LLM | Google Gemini |
| Chat interface | Streamlit |
| Infrastructure | Docker |

---

## Data Types Ingested

| Endpoint | Table | Content |
|---|---|---|
| `/sleep` | `oura_sleep` | Detailed sleep with HRV/HR 5-min timeseries |
| `/daily_sleep` | `oura_daily_sleep` | Sleep score and contributors |
| `/daily_readiness` | `oura_daily_readiness` | Readiness score, temperature deviation |
| `/daily_activity` | `oura_daily_activity` | Steps, calories, activity breakdown |
| `/daily_stress` | `oura_daily_stress` | Stress and recovery minutes |
| `/heartrate` | `oura_heartrate` | Continuous heart rate |
| `/workout` | `oura_workouts` | Workout sessions with intensity |
| `/enhanced_tag` | `oura_events` | Symptoms, lifestyle tags |
| *(derived)* | `oura_cycles` | Cycle day, phase, migraine/PMS flags |

---

## Repository Structure

```
oura-health-analytics/
├── README.md
├── chat.py                                      # Streamlit chat interface
└── workflows/
    ├── Oura-historical-backfill-public.json     # Run once
    ├── Oura-daily-update-public.json            # Scheduled
    └── Oura-AI-chat-public.json                 # AI chat backend
```

---

## Setup

### Prerequisites

- Oura Ring with API v2 personal access token ([get one here](https://cloud.ouraring.com/personal-access-tokens))
- PostgreSQL instance (local or remote)
- n8n self-hosted ([Docker quickstart](https://docs.n8n.io/hosting/installation/docker/))
- Google Gemini API key (example - free, can be changed for preferred AI model e.g., Claude)
- Python 3.9+ with Streamlit

### 1. Database

Create the required tables. Schema file coming in next release — in the meantime the table structures can be inferred from the INSERT statements in the backfill workflow.

### 2. Import workflows into n8n

- Open n8n → **Settings → Import workflow**
- Import all three JSON files from the `workflows/` folder
- In each workflow, update credentials:
  - Replace `YOUR_POSTGRES_CREDENTIAL_NAME` with your PostgreSQL credential
  - Replace `YOUR_OURA_CREDENTIAL_NAME` with your Oura API token credential
  - Replace `YOUR_GEMINI_CREDENTIAL_NAME` with your Gemini credential

### 3. Run historical backfill

- Open `Oura historical backfill` workflow
- In the **Set date range** node, set `START_DATE` to when you got your Oura ring
- Run manually — this loads all historical data and builds the `oura_cycles` table

### 4. Enable daily update

- Open `Oura daily update` workflow
- Enable the schedule trigger (runs daily by default)

### 5. Start the Streamlit interface

```bash
pip install streamlit requests
streamlit run chat.py
```

Make sure n8n is running and the webhook URL in `chat.py` matches your n8n setup:

```python
N8N_WEBHOOK_URL = "http://localhost:5678/webhook/oura/chat"
N8N_RESET_URL   = "http://localhost:5678/webhook/oura/reset"
```

---

## Project Status & Roadmap

This is **v1 — a working prototype** built iteratively.

- [ ] Add `schema.sql` for full database reproducibility
- [ ] Add `requirements.txt`
- [ ] Python rebuild: replace n8n orchestration with pure Python ingestion scripts for a cleaner, fully version-controlled pipeline
- [ ] Streamlit dashboard with visualisations alongside the chat interface
- [ ] Unit tests for SQL query logic
- [ ] docker-compose setup bundling n8n + PostgreSQL

---

## Privacy Note

All data stays local — self-hosted n8n and local PostgreSQL. The only external calls are to the Oura API (to fetch your data) and the LLM APIs (query context only, no raw biometric timeseries are sent).

---

## Author

Built by a biomedical scientist exploring applied data science through personal health analytics.

*Feedback and questions welcome via GitHub Issues.*
