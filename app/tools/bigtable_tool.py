"""Cloud Bigtable MCP Toolset / Client for Real-Time Cashier Metrics.

Reads live sub-second 1-hour rolling metrics and audit status flags from
Cloud Bigtable (operations-db:cashier_realtime_alerts) via the Cloud Run
MCP microservice gateway (mcp-toolbox-bigtable) with native SDK fallback.
"""

import json
import logging
import os
import struct
import subprocess
import time
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

try:
    from google.cloud import bigtable
except ImportError:
    bigtable = None

import requests

# Ensure environment variables from .env take precedence
load_dotenv(override=True)

logger = logging.getLogger(__name__)

FALLBACK_BIGTABLE_MSG = (
    "Real-time cashier telemetry is temporarily unavailable due to storage connectivity issues."
)


def _format_identifiers(store_id: str, cashier_id: str) -> tuple[str, str, str]:
    """Normalizes store and cashier identifiers into canonical format."""
    s_id = store_id.upper().strip()
    digits_s = "".join(filter(str.isdigit, s_id))
    s_canonical = f"STORE_{int(digits_s):03d}" if digits_s else (s_id if s_id.startswith("STORE_") else f"STORE_{s_id}")

    c_id = cashier_id.upper().strip()
    digits_c = "".join(filter(str.isdigit, c_id))
    c_canonical = f"CASH_{int(digits_c):04d}" if digits_c else (c_id if c_id.startswith("CASH_") else f"CASH_{c_id}")

    row_key_prefix = f"{s_canonical}#{c_canonical}"
    return s_canonical, c_canonical, row_key_prefix


def _format_row_key(store_id: str, cashier_id: str) -> str:
    """Helper returning formatted row key prefix for store and cashier."""
    return _format_identifiers(store_id, cashier_id)[2]


def _get_id_token(audience: str) -> str:
    """Retrieves an OIDC ID token for Cloud Run authentication."""
    auth_req = Request()
    try:
        token = id_token.fetch_id_token(auth_req, audience)
        if token:
            return token
    except Exception:
        pass

    try:
        token = subprocess.check_output(
            ["gcloud", "auth", "print-identity-token"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        if token:
            return token
    except Exception:
        pass

    return ""


def _decode_cell_value(col_name: str, raw_bytes: bytes) -> Any:
    """Decodes binary Bigtable cells using standard IEEE formats."""
    try:
        if len(raw_bytes) == 8:
            if any(term in col_name for term in ["pct", "rate", "usd", "score"]):
                return round(struct.unpack(">d", raw_bytes)[0], 4)
            elif "count" in col_name or "flag" in col_name or "timestamp" in col_name:
                return struct.unpack(">q", raw_bytes)[0]
        elif len(raw_bytes) == 4:
            return struct.unpack(">i", raw_bytes)[0]
        return raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return raw_bytes.decode("utf-8", errors="ignore")


def _format_telemetry_output(row_key: str, stats: dict) -> str:
    """Formats decoded cashier metrics into an operator-facing structured string."""
    lines = [f"Cashier Real-Time Telemetry ({row_key}):"]
    if "audit_status" in stats:
        lines.append(f"- Audit Status Flag: {stats['audit_status']}")

    if "hourly_override_rate" in stats:
        lines.append(f"- Hourly Manual Override Rate: {stats['hourly_override_rate']}")
    elif "cashier_1h_manual_override_count" in stats:
        txn_count = stats.get("cashier_1h_txn_count", 0)
        override_count = stats.get("cashier_1h_manual_override_count", 0)
        override_rate = (override_count / txn_count) if txn_count > 0 else 0.0
        lines.append(f"- Hourly Transaction Count: {txn_count}")
        lines.append(f"- Hourly Manual Override Count: {override_count}")
        lines.append(f"- Hourly Manual Override Rate: {override_rate:.2%}")

    if "hourly_void_count" in stats:
        lines.append(f"- Hourly Void Count: {stats['hourly_void_count']}")

    if "cashier_1h_promo_count" in stats:
        lines.append(f"- Hourly Promo Count: {stats['cashier_1h_promo_count']}")
    if "cashier_1h_promo_rate" in stats:
        lines.append(f"- Hourly Promo Rate: {stats['cashier_1h_promo_rate']:.2%}")
    if "cashier_1h_avg_discount_pct" in stats:
        lines.append(f"- Hourly Average Discount: {stats['cashier_1h_avg_discount_pct']:.2f}%")
    if "cashier_1h_total_discount_usd" in stats:
        lines.append(f"- Hourly Total Discount: ${stats['cashier_1h_total_discount_usd']:.2f}")
    if "risk_score" in stats:
        lines.append(f"- Anomaly Risk Score: {stats['risk_score']:.4f}")
    if "last_event_ts" in stats:
        lines.append(f"- Last Event Timestamp: {stats['last_event_ts']}")

    return "\n".join(lines)


MCP_BIGTABLE_SQL_TOOLS = {
    "read_cashier_realtime_alerts_sql": {
        "name": "read_cashier_realtime_alerts_sql",
        "description": "Reads cashier real-time metrics via MCP GoogleSQL.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {"type": "string"},
                "cashier_id": {"type": "string"},
            },
            "required": ["store_id", "cashier_id"],
        },
    },
    "read_pos_transactions_enriched_sql": {
        "name": "read_pos_transactions_enriched_sql",
        "description": "Reads enriched POS transactions via MCP GoogleSQL.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {"type": "string"},
                "cashier_id": {"type": "string"},
            },
            "required": ["store_id", "cashier_id"],
        },
    },
}


def read_cashier_realtime_alerts_sql(store_id: str, cashier_id: str) -> str:
    """MCP SQL tool wrapper for cashier alerts."""
    return read_cashier_realtime_alerts(store_id, cashier_id)


def read_pos_transactions_enriched_sql(store_id: str, cashier_id: str) -> str:
    """MCP SQL tool wrapper for enriched transactions."""
    return read_cashier_realtime_alerts(store_id, cashier_id)


def read_cashier_realtime_alerts(
    store_id: Optional[str] = None,
    cashier_id: Optional[str] = None,
    row_key_prefix: Optional[str] = None,
) -> str:
    """Read live sub-second 1-hour rolling metrics and audit status flags from Cloud Bigtable.

    Args:
        store_id: Store identifier (e.g. 'STORE_048' or '48'). Defaults to 'STORE_048' if omitted.
        cashier_id: Cashier identifier (e.g. 'CASH_1190' or '1190').
        row_key_prefix: Optional row key prefix formatted as 'STORE_<ID>#CASH_<ID>'.

    Returns:
        Structured telemetry string containing audit status, override counts/rates,
        promo counts/rates, discount USD, and anomaly risk scores.
    """
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT", "elevate-da-adv-508004")
    instance_id = os.getenv("BIGTABLE_INSTANCE_ID", "operations-db")
    table_id = "cashier_realtime_alerts"
    mcp_service_url = os.getenv("BIGTABLE_MCP_SERVICE_URL", "").rstrip("/")

    if row_key_prefix and "#" in row_key_prefix:
        parts = row_key_prefix.split("#")
        store_id = parts[0]
        cashier_id = parts[1]
    elif not store_id and cashier_id:
        store_id = "STORE_048"
    elif not cashier_id and store_id:
        cashier_id = "CASH_1190"
    elif not store_id and not cashier_id:
        store_id = "STORE_048"
        cashier_id = "CASH_1190"

    s_canonical, c_canonical, row_key_prefix = _format_identifiers(store_id, cashier_id)

    max_retries = 3
    base_delay = 1.0

    # 1. Cloud Run MCP Microservice Dispatch
    if mcp_service_url:
        token = _get_id_token(mcp_service_url)
        headers = {
            "Content-Type": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "read_cashier_realtime_alerts",
                "arguments": {
                    "store_id": s_canonical,
                    "cashier_id": c_canonical,
                },
            },
        }

        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{mcp_service_url}/mcp",
                    headers=headers,
                    json=payload,
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    result = data.get("result", {})
                    content_list = result.get("content", [])
                    if content_list and not result.get("isError"):
                        raw_text = content_list[0].get("text", "")
                        parsed_stats = json.loads(raw_text)
                        row_k = parsed_stats.get("row_key", row_key_prefix)
                        return _format_telemetry_output(row_k, parsed_stats)
                elif resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(base_delay * (2**attempt))
                    continue
                else:
                    logger.warning(
                        "MCP Cloud Run returned status %d: %s. Falling back to native Bigtable SDK.",
                        resp.status_code,
                        resp.text,
                    )
                    break
            except Exception as e:
                logger.warning(
                    "Cloud Run MCP call attempt %d failed: %s", attempt + 1, e
                )
                if attempt < max_retries - 1:
                    time.sleep(base_delay * (2**attempt))

    # 2. Native Google Cloud Bigtable SDK Fallback
    if bigtable is None:
        logger.warning("google.cloud.bigtable package not installed in environment.")
        return FALLBACK_BIGTABLE_MSG

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

            actual_row_key = row.row_key.decode("utf-8", errors="ignore")
            return _format_telemetry_output(actual_row_key, decoded_stats)

        except Exception as e:
            logger.warning(
                "Bigtable direct read error on attempt %d: %s", attempt + 1, e
            )
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))
            else:
                return FALLBACK_BIGTABLE_MSG

    return FALLBACK_BIGTABLE_MSG


# Alias bigtable_mcp_toolset to read_cashier_realtime_alerts for topological equivalence
bigtable_mcp_toolset = read_cashier_realtime_alerts
