"""Cymbal Retail Analytics BigQuery Conversational Data Agent Tool.

Orchestrates relational analytical queries across structured Gold tables,
extracted warranty policies, and federated AWS S3 datasets via the BigQuery
Conversational Data Agent API and ADK native FunctionTool engine.
"""

import logging
import os
import time
from typing import Any, Callable, Dict, Optional

from dotenv import load_dotenv
import google.auth
from google.auth.transport.requests import Request
import requests

# Ensure environment variables from .env take precedence
load_dotenv(override=True)

try:
    from google.adk.tools import FunctionTool
    from google.adk.tools.data_agent.config import DataAgentToolConfig
    from google.adk.tools.data_agent.data_agent_tool import ask_data_agent
except ImportError:
    # Resilient fallback wrapper compliant with ADK 2.0 FunctionTool interface
    class FunctionTool:  # type: ignore[no-redef]
        """ADK FunctionTool specification wrapper."""

        def __init__(
            self,
            func: Optional[Callable[..., Any]] = None,
            name: Optional[str] = None,
            description: Optional[str] = None,
        ):
            self.func = func
            self.name = name or (func.__name__ if func else "cymbal_analytics_tool")
            self.description = description or (func.__doc__ if func else "")

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            if self.func:
                return self.func(*args, **kwargs)
            return None

    class DataAgentToolConfig:  # type: ignore[no-redef]
        def __init__(self, location: str = "global", **kwargs: Any):
            self.location = location

    def ask_data_agent(*args: Any, **kwargs: Any) -> dict[str, Any]:  # type: ignore[no-redef]
        """Native ADK ask_data_agent execution fallback."""
        return {"status": "ERROR", "error_details": "ADK data agent unavailable"}

logger = logging.getLogger(__name__)

FALLBACK_UNREACHABLE_MSG = (
    "Store analytics data is currently unreachable due to transient database "
    "connectivity issues. Please retry shortly."
)


def _format_agent_response(res: dict[str, Any]) -> str:
    """Formats the Data Agent response structure into a clean, LLM-ready text string."""
    final_texts = []
    generated_sql = None
    table_summary = []

    for item in res.get("response", []):
        if not isinstance(item, dict):
            continue
        if "text" in item and isinstance(item["text"], dict):
            t_type = item["text"].get("textType")
            parts = item["text"].get("parts", [])
            if t_type == "FINAL_RESPONSE":
                final_texts.extend(parts)
        elif "data" in item and isinstance(item["data"], dict):
            if "generatedSql" in item["data"]:
                generated_sql = item["data"]["generatedSql"]
        elif "Data Retrieved" in item and isinstance(item["Data Retrieved"], dict):
            headers = item["Data Retrieved"].get("headers", [])
            rows = item["Data Retrieved"].get("rows", [])
            summary = item["Data Retrieved"].get("summary", "")
            table_summary.append(f"Headers: {headers}\nRows: {rows}\nSummary: {summary}")

    out_parts = []
    if final_texts:
        out_parts.append("\n\n".join(final_texts))
    if generated_sql:
        out_parts.append(f"Generated SQL:\n{generated_sql}")
    if table_summary:
        out_parts.append("Data Summary:\n" + "\n".join(table_summary))

    if out_parts:
        return "\n\n".join(out_parts)

    return str(res.get("response", ""))


def cymbal_analytics_tool(query: str) -> str:
    """Queries the Cymbal Retail Analytics BigQuery Conversational Data Agent in natural language.

    Args:
        query: Verbatim natural language business inquiry referencing retail metrics,
            inventory positions, historical purchases, warranty terms, or cross-cloud audits.

    Returns:
        Structured analytical answer including generated SQL and actual query result rows,
        or a fallback message if transient database connectivity fails.
    """
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    agent_id = os.getenv("DATA_AGENT_ID", "cymbal-retail-agent")
    # Location override: MUST use 'global' to avoid mTLS endpoint routing errors
    location = os.getenv("DATA_AGENT_LOCATION", "global")
    data_agent_name = f"projects/{project_id}/locations/{location}/dataAgents/{agent_id}"

    # Transient fault tolerance with exponential backoff
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            settings = DataAgentToolConfig(location=location)

            res = ask_data_agent(
                data_agent_name=data_agent_name,
                query=query,
                credentials=credentials,
                settings=settings,
                tool_context=None,
            )

            if res.get("status") == "SUCCESS":
                formatted = _format_agent_response(res)
                if formatted:
                    return formatted

            error_msg = res.get("error_details", "")
            logger.warning(
                "Data Agent returned non-success (attempt %d): %s",
                attempt + 1,
                error_msg,
            )
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))

        except Exception as e:
            logger.warning(
                "Exception calling BigQuery Data Agent (attempt %d): %s",
                attempt + 1,
                e,
            )
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))
            else:
                return FALLBACK_UNREACHABLE_MSG

    return FALLBACK_UNREACHABLE_MSG


# Standardize cymbal_analytics_tool on the ADK native FunctionTool wrapper
cymbal_analytics_function_tool = FunctionTool(
    func=cymbal_analytics_tool,
)
