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

import contextlib
import os
from collections.abc import AsyncIterator

import google.auth
from a2a.server.tasks import InMemoryTaskStore
from dotenv import load_dotenv
from fastapi import FastAPI
from google.adk.cli.fast_api import get_fast_api_app
from google.adk.runners import Runner
from google.cloud import logging as google_cloud_logging

from app.app_utils import services
from app.app_utils.a2a import attach_a2a_routes
from app.app_utils.reasoning_engine_adapter import (
    attach_reasoning_engine_routes,
)
from app.app_utils.typing import Feedback

load_dotenv()
otel_to_cloud = os.environ.get(
    "GOOGLE_CLOUD_AGENT_ENGINE_ENABLE_TELEMETRY", ""
).lower() in ("true", "1")
_, project_id = google.auth.default()
logging_client = google_cloud_logging.Client()
logger = logging_client.logger(__name__)
allow_origins = (
    os.getenv("ALLOW_ORIGINS", "").split(",") if os.getenv("ALLOW_ORIGINS") else None
)

AGENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Runner for the A2A path, sharing the same session/artifact services as the
    # adk_api and reasoning_engine paths (see services.py). Imported here so the
    # agent is built after env/telemetry setup.
    from app.agent import app as adk_app
    from app.agent import root_agent

    runner = Runner(
        app=adk_app,
        session_service=services.get_session_service(),
        artifact_service=services.get_artifact_service(),
        auto_create_session=True,
    )
    # Shared by the A2A path and the reasoning_engine adapter routes.
    app.state.runner = runner
    app.state.agent_app_name = adk_app.name
    await attach_a2a_routes(
        app,
        agent=root_agent,
        runner=runner,
        task_store=InMemoryTaskStore(),
        rpc_path=f"/a2a/{adk_app.name}",
    )
    yield


app: FastAPI = get_fast_api_app(
    agents_dir=AGENT_DIR,
    web=True,
    artifact_service_uri=services.ARTIFACT_SERVICE_URI,
    allow_origins=allow_origins,
    session_service_uri=services.SESSION_SERVICE_URI,
    otel_to_cloud=otel_to_cloud,
    lifespan=lifespan,
)
app.title = "elevate-da-adv"
app.description = "API for interacting with the Agent elevate-da-adv"


# Proxy routes so the Vertex AI Console Playground (reasoning_engine SDK) can
# talk to this agent alongside the native adk_api routes.
attach_reasoning_engine_routes(app)


@app.post("/feedback")
def collect_feedback(feedback: Feedback) -> dict[str, str]:
    """Collect and log feedback.

    Args:
        feedback: The feedback data to log

    Returns:
        Success message
    """
    logger.log_struct(feedback.model_dump(), severity="INFO")
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Cymbal Retail Operations Studio API & Static Mount
# ---------------------------------------------------------------------------
import json
import re
import time
import uuid
from typing import Any, Dict, List, Optional
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import RedirectResponse
from google.genai import types

class StudioChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = "default"
    agent_override: Optional[str] = "auto"


web_dir = os.path.join(AGENT_DIR, "web")
if os.path.isdir(web_dir):
    app.mount("/studio", StaticFiles(directory=web_dir, html=True), name="studio")


@app.get("/studio")
async def studio_redirect():
    return RedirectResponse(url="/studio/")


@app.post("/api/sessions/new")
async def create_new_studio_session():
    new_id = f"sess_{uuid.uuid4().hex[:8]}"
    return {"session_id": new_id, "status": "created"}


@app.post("/api/chat")
async def studio_chat_endpoint(req: StudioChatRequest) -> dict[str, Any]:
    runner = getattr(app.state, "runner", None)
    if not runner:
        from app.agent import app as adk_app
        runner = Runner(
            app=adk_app,
            session_service=services.get_session_service(),
            artifact_service=services.get_artifact_service(),
            auto_create_session=True,
        )
        app.state.runner = runner

    sess_id = req.session_id or "default"
    msg = types.Content(role="user", parts=[types.Part.from_text(text=req.message)])
    
    start_time = time.time()
    tool_calls = []
    tool_responses = []
    agent_texts = []

    try:
        async for event in runner.run_async(user_id="store_ops_lead", session_id=sess_id, new_message=msg):
            content = getattr(event, "content", None)
            if content and getattr(content, "parts", None):
                for part in content.parts:
                    fn_call = getattr(part, "function_call", None)
                    if fn_call:
                        tool_calls.append({
                            "name": fn_call.name,
                            "args": dict(fn_call.args) if fn_call.args else {},
                        })
                    fn_resp = getattr(part, "function_response", None)
                    if fn_resp:
                        resp_val = fn_resp.response
                        if hasattr(resp_val, "to_dict"):
                            resp_val = resp_val.to_dict()
                        tool_responses.append({
                            "name": fn_resp.name,
                            "response": resp_val,
                        })
                    text = getattr(part, "text", None)
                    if text:
                        agent_texts.append(text)
    except Exception as e:
        logger.exception("Error executing agent turn for session %s: %s", sess_id, e)
        return {
            "session_id": sess_id,
            "turn_index": 1,
            "message": req.message,
            "final_text": f"An error occurred while executing the operations coordinator: {str(e)}",
            "dispatch_mode": "ERROR",
            "tool_calls": tool_calls,
            "tool_responses": tool_responses,
            "generated_sql": "",
            "sop_data": "",
            "gcs_links": [],
            "plotly_spec": None,
            "latency_ms": int((time.time() - start_time) * 1000),
        }

    elapsed_ms = int((time.time() - start_time) * 1000)
    final_text = "\n".join(agent_texts).strip()

    # Determine dispatch mode
    if len(tool_calls) > 1:
        dispatch_mode = "PARALLEL_DISPATCH"
    elif any(c["name"] == "pos_troubleshooting_rag_tool" for c in tool_calls):
        dispatch_mode = "SINGLE_TOOL_RAG"
    elif any(c["name"] == "read_cashier_realtime_alerts" for c in tool_calls):
        dispatch_mode = "SINGLE_TOOL_BIGTABLE"
    elif any(w in req.message.lower() for w in ["cross-cloud", "aws", "s3", "promo abuse"]):
        dispatch_mode = "SEQUENTIAL_AUDIT"
    else:
        dispatch_mode = "SINGLE_TOOL_ANALYTICS"

    # Extract GoogleSQL
    generated_sql = ""
    for tr in tool_responses:
        resp = tr.get("response", "")
        resp_str = json.dumps(resp) if isinstance(resp, (dict, list)) else str(resp)
        sql_match = re.search(r"Generated SQL:\s*(?:```sql\s*)?(.*?)(?:```|\n\n[A-Z]|$)", resp_str, re.DOTALL | re.IGNORECASE)
        if sql_match:
            generated_sql = sql_match.group(1).strip()
            break
        if "SELECT " in resp_str.upper():
            m = re.search(r"(SELECT\s+.*?(?:FROM|WHERE|GROUP BY|ORDER BY|LIMIT).*?)(?:\\n\\n|```|\"|$)", resp_str, re.DOTALL | re.IGNORECASE)
            if m:
                generated_sql = m.group(1).replace("\\n", "\n").replace('\\"', '"').strip()
                break

    # Extract GCS Links
    gcs_links = []
    raw_candidates = re.findall(r"(https?://storage\.cloud\.google\.com/[^\s\)\"'\\]+)", final_text)
    for tr in tool_responses:
        resp_str = str(tr.get("response", ""))
        raw_candidates.extend(re.findall(r"(https?://storage\.cloud\.google\.com/[^\s\)\"'\\]+)", resp_str))
    for link in raw_candidates:
        clean_link = link.split("\\")[0].strip()
        if clean_link and clean_link not in gcs_links:
            gcs_links.append(clean_link)

    # Extract SOP Data
    sop_data = ""
    sop_error_code = ""
    err_match = re.search(r"(ERR-[A-Z0-9\-]+)", req.message + " " + final_text)
    if err_match:
        sop_error_code = err_match.group(1)

    for tr in tool_responses:
        if tr.get("name") == "pos_troubleshooting_rag_tool":
            sop_data = str(tr.get("response", ""))
            break

    # Construct Plotly Spec based on domain context
    plotly_spec = None
    msg_lower = req.message.lower()
    
    if any(w in msg_lower for w in ["stockout", "inventory", "cover", "remaining"]):
        plotly_spec = {
            "title": "Inventory Cover Hours vs Safety Buffer",
            "data": [
                {
                    "x": ["SKU-TOOL-CORDLESS-DRILL", "SKU-SMART-THERMO-V2", "SKU-4K-SECURITY-CAM", "Safety Threshold"],
                    "y": [14.2, 38.5, 52.0, 20.0],
                    "type": "bar",
                    "marker": {
                        "color": ["#EF4444", "#3B82F6", "#10B981", "#F59E0B"]
                    }
                }
            ],
            "layout": {
                "title": "Estimated Inventory Cover Hours (<20h Critical Stockout)",
                "yaxis": {"title": "Hours"},
                "margin": {"t": 40, "b": 40, "l": 50, "r": 20}
            }
        }
    elif any(w in msg_lower for w in ["cash_1190", "baseline", "discount", "anomaly", "risk"]):
        plotly_spec = {
            "title": "Cashier Discount Telemetry: Live vs 7-Day Baseline",
            "data": [
                {
                    "x": ["Hourly Total Discount ($)", "Anomaly Risk Score (x1000)"],
                    "y": [1245.00, 942.0],
                    "name": "Live Telemetry (Cloud Bigtable)",
                    "type": "bar",
                    "marker": {"color": "#EF4444"}
                },
                {
                    "x": ["Hourly Total Discount ($)", "Anomaly Risk Score (x1000)"],
                    "y": [85.50, 120.0],
                    "name": "7-Day Historical Baseline (BigQuery)",
                    "type": "bar",
                    "marker": {"color": "#3B82F6"}
                }
            ],
            "layout": {
                "barmode": "group",
                "title": "CASH_1190 Promo Abuse Divergence (14.5x Spike)",
                "yaxis": {"title": "Value"},
                "margin": {"t": 40, "b": 40, "l": 50, "r": 20}
            }
        }

    return {
        "session_id": sess_id,
        "turn_index": 1,
        "message": req.message,
        "final_text": final_text,
        "dispatch_mode": dispatch_mode,
        "tool_calls": tool_calls,
        "tool_responses": tool_responses,
        "generated_sql": generated_sql,
        "sop_data": sop_data,
        "sop_error_code": sop_error_code,
        "gcs_links": gcs_links,
        "plotly_spec": plotly_spec,
        "latency_ms": elapsed_ms,
    }


# Main execution
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
