"""Cymbal Retail Analytics BigQuery Conversational Data Agent Tool.

Orchestrates relational analytical queries across structured Gold tables,
extracted warranty policies, and federated AWS S3 datasets via the BigQuery
Conversational Data Agent API and ADK native FunctionTool engine.
"""

import logging
import os
import time
from typing import Any, Callable, Dict, Optional

import google.auth
from google.auth.transport.requests import Request
import requests

try:
    from google.adk.tools import FunctionTool
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

    def ask_data_agent(data_agent_name: str, query: str) -> str:  # type: ignore[no-redef]
        """Native ADK ask_data_agent execution fallback."""
        return ""

logger = logging.getLogger(__name__)

FALLBACK_UNREACHABLE_MSG = (
    "Store analytics data is currently unreachable due to transient database "
    "connectivity issues. Please retry shortly."
)


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
    agent_id = os.getenv("DATA_AGENT_ID", "cymbal-data-agent")
    # Location override: MUST use 'global' to avoid mTLS endpoint routing errors
    location = os.getenv("DATA_AGENT_LOCATION", "global")
    data_agent_name = f"projects/{project_id}/locations/{location}/dataAgents/{agent_id}"

    # Transient fault tolerance with exponential backoff
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            # 1. First attempt execution via native ADK ask_data_agent if available
            try:
                native_result = ask_data_agent(data_agent_name=data_agent_name, query=query)
                if native_result:
                    return str(native_result)
            except Exception:
                pass

            # 2. Resilient authenticated REST fallback to Gemini Data Analytics v1beta
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            credentials.refresh(Request())

            headers = {
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
                "X-Goog-API-Client": "GOOGLE_ADK",
            }

            base_url = "https://geminidataanalytics.googleapis.com/v1beta"
            url = f"{base_url}/{data_agent_name}:query"

            # Pass verbatim natural language query without keyword stripping
            payload = {"query": query}

            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                result_data = resp.json()
                return str(result_data)
            elif resp.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "Data Agent transient error status %d on attempt %d: %s",
                    resp.status_code,
                    attempt + 1,
                    resp.text,
                )
                time.sleep(base_delay * (2**attempt))
                continue
            else:
                logger.warning(
                    "Data Agent permanent error status %d: %s",
                    resp.status_code,
                    resp.text,
                )
                return FALLBACK_UNREACHABLE_MSG

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
    name="cymbal_analytics_tool",
    description=(
        "Queries the Cymbal Retail Analytics BigQuery Conversational Data Agent in natural language. "
        "Orchestrates relational analytical queries across structured Gold tables, extracted warranty "
        "policies, and federated AWS S3 datasets via ADK ask_data_agent engine."
    ),
)
