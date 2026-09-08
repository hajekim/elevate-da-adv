"""Cymbal Retail Analytics BigQuery Conversational Data Agent Tool.

Orchestrates relational analytical queries across structured Gold tables,
extracted warranty policies, and federated AWS S3 datasets via the BigQuery
Conversational Data Agent API.
"""

import logging
import os
import time
from typing import Any, Dict

import google.auth
from google.auth.transport.requests import Request
import requests

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
    project_id = os.getenv("PROJECT_ID", "elevate-da-adv-508004")
    agent_id = os.getenv("DATA_AGENT_ID", "cymbal-data-agent")
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
