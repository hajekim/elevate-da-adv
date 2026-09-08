"""Cloud Bigtable MCP Toolset / Client for Real-Time Cashier Metrics.

Reads live sub-second 1-hour rolling metrics and audit status flags from
Cloud Bigtable (operations-db:cashier_realtime_alerts) or via the Cloud Run
MCP microservice gateway (mcp-toolbox-bigtable).
"""

import logging
import os
import struct
import time
from typing import Any, Dict, Optional

import google.auth
from google.auth.transport.requests import Request
from google.cloud import bigtable
import requests

logger = logging.getLogger(__name__)

FALLBACK_BIGTABLE_MSG = (
    "Real-time cashier telemetry is temporarily unavailable due to storage connectivity issues."
)


def _format_row_key(store_id: str, cashier_id: str) -> str:
    """Normalizes store and cashier identifiers into STORE_<ID>#CASH_<ID> format."""
    s_id = store_id.upper().strip()
    if not s_id.startswith("STORE_"):
        # Pad digits if necessary, e.g. "48" -> "STORE_048"
        digits = "".join(filter(str.isdigit, s_id))
        s_id = f"STORE_{int(digits):03d}" if digits else f"STORE_{s_id}"

    c_id = cashier_id.upper().strip()
    if not c_id.startswith("CASH_"):
        digits = "".join(filter(str.isdigit, c_id))
        c_id = f"CASH_{int(digits):04d}" if digits else f"CASH_{c_id}"

    return f"{s_id}#{c_id}"


def _decode_cell_value(col_name: str, raw_bytes: bytes) -> Any:
    """Decodes binary Bigtable cells using standard IEEE formats."""
    try:
        if len(raw_bytes) == 8:
            if "rate" in col_name or "score" in col_name or "ratio" in col_name:
                return round(struct.unpack(">d", raw_bytes)[0], 4)
            elif "count" in col_name or "flag" in col_name or "timestamp" in col_name:
                return struct.unpack(">q", raw_bytes)[0]
        elif len(raw_bytes) == 4:
            return struct.unpack(">i", raw_bytes)[0]
        return raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return raw_bytes.decode("utf-8", errors="ignore")


def read_cashier_realtime_alerts(store_id: str, cashier_id: str) -> str:
    """Read live sub-second 1-hour rolling metrics and audit status flags from Cloud Bigtable.

    Args:
        store_id: Store identifier (e.g. 'STORE_048' or '48').
        cashier_id: Cashier identifier (e.g. 'CASH_1190' or '1190').

    Returns:
        Structured string containing hourly scan rate, hourly override rate,
        hourly void count, anomaly score, and audit flag status.
    """
    project_id = os.getenv("PROJECT_ID", "elevate-da-adv-508004")
    instance_id = os.getenv("BIGTABLE_INSTANCE_ID", "operations-db")
    table_id = "cashier_realtime_alerts"
    mcp_service_url = os.getenv("BIGTABLE_MCP_SERVICE_URL", "")

    row_key_prefix = _format_row_key(store_id, cashier_id)

    max_retries = 3
    base_delay = 1.0

    # 1. Cloud Run MCP Microservice Dispatch (if URL configured)
    if mcp_service_url:
        for attempt in range(max_retries):
            try:
                credentials, _ = google.auth.default()
                auth_req = Request()
                credentials.refresh(auth_req)

                headers = {
                    "Authorization": f"Bearer {credentials.token}",
                    "Content-Type": "application/json",
                }

                endpoint = f"{mcp_service_url.rstrip('/')}/read_metrics"
                resp = requests.post(
                    endpoint,
                    headers=headers,
                    json={"row_key_prefix": row_key_prefix},
                    timeout=10,
                )
                if resp.status_code == 200:
                    return str(resp.json())
                time.sleep(base_delay * (2**attempt))
            except Exception as e:
                logger.warning(
                    "Cloud Run MCP call failed on attempt %d: %s", attempt + 1, e
                )
                time.sleep(base_delay * (2**attempt))

    # 2. Native Google Cloud Bigtable SDK Fallback
    for attempt in range(max_retries):
        try:
            client = bigtable.Client(project=project_id, admin=False)
            instance = client.instance(instance_id)
            table = instance.table(table_id)

            # Query rows with prefix row_key_prefix
            end_key = row_key_prefix[:-1] + chr(ord(row_key_prefix[-1]) + 1)
            row_set = bigtable.row_set.RowSet()
            row_set.add_row_range_from_keys(
                start_key=row_key_prefix.encode("utf-8"),
                end_key=end_key.encode("utf-8"),
            )

            rows = list(table.read_rows(row_set=row_set, limit=1))
            if not rows:
                return (
                    f"No active real-time alert record found in Cloud Bigtable for "
                    f"Cashier '{cashier_id}' at Store '{store_id}' (Row key prefix: {row_key_prefix})."
                )

            row = rows[0]
            decoded_stats = {}
            for cf_name, cols in row.cells.items():
                for col_name_bytes, cell_list in cols.items():
                    col_str = col_name_bytes.decode("utf-8", errors="ignore")
                    latest_cell = cell_list[0]
                    decoded_stats[col_str] = _decode_cell_value(col_str, latest_cell.value)

            return (
                f"Cashier Real-Time Telemetry ({row_key_prefix}):\n"
                f"- Hourly Scan Rate: {decoded_stats.get('hourly_scan_rate', 'N/A')}\n"
                f"- Hourly Manual Override Rate: {decoded_stats.get('hourly_override_rate', 'N/A')}\n"
                f"- Hourly Void Count: {decoded_stats.get('hourly_void_count', 'N/A')}\n"
                f"- Anomaly Risk Score: {decoded_stats.get('anomaly_score', 'N/A')}\n"
                f"- Audit Flag Status: {decoded_stats.get('audit_flag', 'NORMAL')}"
            )

        except Exception as e:
            logger.warning(
                "Bigtable direct read error on attempt %d: %s", attempt + 1, e
            )
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))
            else:
                return FALLBACK_BIGTABLE_MSG

    return FALLBACK_BIGTABLE_MSG
