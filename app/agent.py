# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cymbal Retail Operations Coordinator Agent.

Orchestrates 3 decoupled tool gateways:
1. BigQuery Conversational Data Agent (cymbal_analytics_tool)
2. POS Manual Vector RAG with Adjacent Context Stitching (pos_troubleshooting_rag_tool)
3. Cloud Bigtable Sub-second Cashier Telemetry (read_cashier_realtime_alerts)
"""

import logging
import os

try:
    from google.adk.agents import Agent
    from google.adk.apps import App
    from google.adk.models import Gemini
    from google.genai import types
except ImportError:
    # Minimal fallback mock classes for environments where google-adk is imported differently
    class Agent:
        def __init__(self, name, model=None, instruction="", tools=None):
            self.name = name
            self.model = model
            self.instruction = instruction
            self.tools = tools or []

    class App:
        def __init__(self, name="cymbal_operations_agent", root_agent=None, agent=None, plugins=None):
            self.name = name
            self.root_agent = root_agent or agent
            self.plugins = plugins or []

    class Gemini:
        def __init__(self, model, retry_options=None):
            self.model = model
            self.retry_options = retry_options

    class _Types:
        class HttpRetryOptions:
            def __init__(self, attempts=3):
                self.attempts = attempts

    types = _Types()

from .tools.analytics_tool import cymbal_analytics_tool
from .tools.bigtable_tool import read_cashier_realtime_alerts
from .tools.rag_tool import pos_troubleshooting_rag_tool

logger = logging.getLogger(__name__)

MODEL = os.getenv("AGENT_MODEL", "gemini-3.6-flash")

SYSTEM_INSTRUCTION = """You are the Cymbal Retail Operations Coordinator Agent (`cymbal_operations_agent`).
You orchestrate 3 specialized tool gateways to assist store leads, technicians, and loss-prevention auditors:

1. `cymbal_analytics_tool`: BigQuery Conversational Data Agent for relational analytics across structured Gold tables (`pos_transactions_gold`, `pos_anomaly_alerts`, `gold_inventory_reconciliation_ledger`, `historical_transactional_data`), extracted warranty policies (`warranty_generic_sections_extracted`), and federated AWS S3 tables (`silver_pos_transactions`).
   - Always pass standardized business terms verbatim (e.g. Net Transaction Revenue, Total On-Hand Inventory, Estimated Inventory Cover Hours).

2. `pos_troubleshooting_rag_tool`: Vector similarity search with adjacent context window stitching over POS hardware manuals in BigQuery (`cymbal_gold.pos_manual_chunk_embeddings`).
   - Use for hardware error codes (e.g. ERR-PAY-4001, ERR-DN-PRNT-24V) and terminal maintenance SOPs.
   - Always include the clickable HTTPS GCS manual link in your response.

3. `read_cashier_realtime_alerts`: Live sub-second 1-hour rolling metrics and audit status flags from Cloud Bigtable (`operations-db:cashier_realtime_alerts`).
   - Use row key prefix format `STORE_<ID>#CASH_<ID>` (e.g. `STORE_048#CASH_1190`).

TOOL DISPATCH PROTOCOLS:
- SINGLE-TOOL DISPATCH: Route direct inquiries to the appropriate tool. For ANY technical, maintenance, or repair question (including out-of-domain vehicle/machinery repair like Ford F-150 oil change), you MUST invoke `pos_troubleshooting_rag_tool` first. If the RAG tool returns a refusal message indicating no certified rules were found, reply ONLY with that exact refusal sentence and do NOT add unverified conversational advice or self-introductions.
- PARALLEL TOOL DISPATCH: When asked to compare live real-time cashier metrics against historical 7-day baselines (e.g. UC 2.2), invoke `read_cashier_realtime_alerts` AND `cymbal_analytics_tool` concurrently in the same turn.
- SEQUENTIAL MULTI-TURN DISPATCH: When auditing cross-cloud promo abuse offenders (e.g. UC 2.3), first call `cymbal_analytics_tool` to rank top promo abuse offenders in GCP BigQuery (`pos_anomaly_alerts`), then invoke `cymbal_analytics_tool` to retrieve checkout logs from AWS S3 (`silver_pos_transactions`) for the top offending cashier.
- STRICT GROUNDING: Base every sentence of your final response strictly on the data returned by the invoked tools. Avoid ungrounded introductory or concluding conversational filler.
"""

retry_options = getattr(types, "HttpRetryOptions", None)
retry_cfg = retry_options(attempts=3) if retry_options else None

root_agent = Agent(
    name="cymbal_operations_agent",
    model=Gemini(
        model=MODEL,
        retry_options=retry_cfg,
    ),
    instruction=SYSTEM_INSTRUCTION,
    tools=[
        cymbal_analytics_tool,
        pos_troubleshooting_rag_tool,
        read_cashier_realtime_alerts,
    ],
)

# BigQuery Agent Analytics Telemetry Plugin
_plugins = []
_project_id = os.environ.get("PROJECT_ID", "elevate-da-adv-508004")
_dataset_id = os.environ.get("BQ_TELEMETRY_DATASET", "agent_telemetry")
_location = os.environ.get("REGION", "us-central1")

try:
    from google.adk.plugins.bigquery_agent_analytics_plugin import (
        BigQueryAgentAnalyticsPlugin,
        BigQueryLoggerConfig,
    )

    _plugins.append(
        BigQueryAgentAnalyticsPlugin(
            project_id=_project_id,
            dataset_id=_dataset_id,
            location=_location,
            config=BigQueryLoggerConfig(),
        )
    )
except ImportError:
    logger.info("BigQueryAgentAnalyticsPlugin not available in environment; skipping telemetry plugin initialization.")

app = App(
    name="cymbal_operations_agent",
    root_agent=root_agent,
    plugins=_plugins,
)
