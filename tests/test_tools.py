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

"""Unit and integration tests for specialized agent tools and gateways."""

import struct
import unittest
from unittest.mock import MagicMock, patch

from app.tools.analytics_tool import (
    FALLBACK_UNREACHABLE_MSG,
    cymbal_analytics_function_tool,
    cymbal_analytics_tool,
)
from app.tools.bigtable_tool import (
    FALLBACK_BIGTABLE_MSG,
    MCP_BIGTABLE_SQL_TOOLS,
    _decode_cell_value,
    _format_row_key,
    read_cashier_realtime_alerts,
    read_cashier_realtime_alerts_sql,
    read_pos_transactions_enriched_sql,
)
from app.tools.rag_tool import (
    CERTIFIED_REFUSAL_MSG,
    ERROR_CODE_REGEX,
    SIMILARITY_THRESHOLD,
    _clean_query_tokens,
    _convert_gcs_uri_to_https,
    pos_troubleshooting_rag_tool,
)


class TestPosRagTool(unittest.TestCase):
    """Test suite for POS hardware troubleshooting RAG tool."""

    def test_certified_refusal_string_exact_match(self):
        """Verifies exact adherence to SDD mandatory refusal string."""
        expected = (
            "I cannot find certified warranty or repair rules for this specific error "
            "in our technical repository."
        )
        self.assertEqual(CERTIFIED_REFUSAL_MSG, expected)

    def test_error_code_regex_matching(self):
        """Verifies error code regex pattern extraction."""
        m1 = ERROR_CODE_REGEX.search("How do I fix ERR-PAY-4001 on terminal 2?")
        self.assertIsNotNone(m1)
        self.assertEqual(m1.group(0), "ERR-PAY-4001")

        m2 = ERROR_CODE_REGEX.search("Terminal shows error ERR-DN-PRNT-24V power fault")
        self.assertIsNotNone(m2)
        self.assertEqual(m2.group(0), "ERR-DN-PRNT-24V")

        m3 = ERROR_CODE_REGEX.search("How to change oil on a Ford F-150?")
        self.assertIsNone(m3)

    def test_clean_query_tokens(self):
        """Verifies stop-word removal for fallback search."""
        cleaned = _clean_query_tokens("How do I fix the terminal print head error?")
        self.assertNotIn("how", cleaned.split())
        self.assertNotIn("the", cleaned.split())
        self.assertIn("terminal", cleaned)
        self.assertIn("print", cleaned)

    def test_convert_gcs_uri_to_https(self):
        """Verifies GCS URI to HTTPS conversion."""
        self.assertEqual(
            _convert_gcs_uri_to_https("gs://cymbal-pos-manuals/manual.pdf"),
            "https://storage.cloud.google.com/cymbal-pos-manuals/manual.pdf",
        )
        self.assertEqual(_convert_gcs_uri_to_https(""), "")

    @patch("google.cloud.bigquery.Client")
    def test_rag_refusal_on_low_similarity(self, mock_bq_client_cls):
        """Verifies strict refusal when vector similarity is below 0.70 and fallback fails."""
        mock_client = MagicMock()
        mock_bq_client_cls.return_value = mock_client

        # Mock vector search returning low similarity (0.55 < 0.70)
        mock_row = MagicMock()
        mock_row.similarity_score = 0.55
        mock_row.stitched_procedure = "Generic SOP text"
        mock_row.document_title = "General Manual"
        mock_row.document_filename = "gen.pdf"
        mock_row.equipment_covered = "POS"
        mock_row.source_pdf_uri = "gs://bkt/gen.pdf"

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [mock_row]
        mock_client.query.return_value = mock_query_job

        # Query with unmatched error code should NOT boost and should trigger refusal
        result = pos_troubleshooting_rag_tool("ERR-SYNC-900")
        self.assertEqual(result, CERTIFIED_REFUSAL_MSG)

    @patch("google.cloud.bigquery.Client")
    def test_rag_boost_on_matching_error_code(self, mock_bq_client_cls):
        """Verifies 0.99 boost when error code matches in retrieved procedure."""
        mock_client = MagicMock()
        mock_bq_client_cls.return_value = mock_client

        mock_row = MagicMock()
        mock_row.similarity_score = 0.65  # Below 0.70 initially
        mock_row.chunk_content = "Resolution steps for ERR-PAY-4001: reset PIN pad terminal."
        mock_row.stitched_procedure = "Resolution steps for ERR-PAY-4001: reset PIN pad terminal."
        mock_row.document_title = "Payment Terminal Runbook"
        mock_row.document_filename = "pinpad.pdf"
        mock_row.equipment_covered = "CymbalPay Terminal"
        mock_row.source_pdf_uri = "gs://cymbal-pos-manuals/pinpad.pdf"

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [mock_row]
        mock_client.query.return_value = mock_query_job

        result = pos_troubleshooting_rag_tool("ERR-PAY-4001")
        self.assertIn("Similarity Score: 0.9900", result)
        self.assertIn("Payment Terminal Runbook", result)
        self.assertIn("https://storage.cloud.google.com/cymbal-pos-manuals/pinpad.pdf", result)


class TestCymbalAnalyticsTool(unittest.TestCase):
    """Test suite for BigQuery Conversational Data Agent analytics tool."""

    @patch("app.tools.analytics_tool.ask_data_agent")
    def test_analytics_tool_success(self, mock_ask):
        """Verifies successful analytics query execution."""
        mock_ask.return_value = {
            "status": "SUCCESS",
            "response": [
                {
                    "text": {
                        "parts": ["Total daily revenue for STORE_008 is $154,200.50."],
                        "textType": "FINAL_RESPONSE",
                    }
                },
                {
                    "data": {
                        "generatedSql": "SELECT SUM(net_amount) AS total_revenue FROM cymbal_gold.pos_transactions_gold WHERE store_id = 'STORE_008'"
                    }
                },
                {
                    "Data Retrieved": {
                        "headers": ["total_revenue"],
                        "rows": [[154200.50]],
                        "summary": "Showing 1 row.",
                    }
                },
            ],
        }

        res = cymbal_analytics_tool("STORE_008 total daily revenue")
        self.assertIn("total_revenue", res)
        self.assertIn("154200.5", res)

    @patch("app.tools.analytics_tool.ask_data_agent")
    def test_analytics_tool_fallback_on_unreachable(self, mock_ask):
        """Verifies graceful fallback message when remote endpoint is unreachable."""
        mock_ask.side_effect = Exception("Connection timed out")
        res = cymbal_analytics_tool("any retail query")
        self.assertEqual(res, FALLBACK_UNREACHABLE_MSG)

    def test_function_tool_wrapper(self):
        """Verifies ADK FunctionTool specification compliance and direct execution."""
        self.assertEqual(cymbal_analytics_function_tool.name, "cymbal_analytics_tool")
        self.assertIn("BigQuery Conversational Data Agent", cymbal_analytics_function_tool.description)


class TestBigtableTelemetryTool(unittest.TestCase):
    """Test suite for Cloud Bigtable real-time telemetry tool and MCP schemas."""

    def test_format_row_key_normalization(self):
        """Verifies normalization to STORE_<ID>#CASH_<ID> format."""
        self.assertEqual(_format_row_key("STORE_048", "CASH_1190"), "STORE_048#CASH_1190")
        self.assertEqual(_format_row_key("48", "1190"), "STORE_048#CASH_1190")
        self.assertEqual(_format_row_key("store_8", "cash_5"), "STORE_008#CASH_0005")

    def test_decode_cell_value_binary_unpacking(self):
        """Verifies IEEE standard struct decoding for floating-point and integer metrics."""
        double_bytes = struct.pack(">d", 0.4215)
        self.assertEqual(_decode_cell_value("hourly_override_rate", double_bytes), 0.4215)

        int_bytes = struct.pack(">q", 12)
        self.assertEqual(_decode_cell_value("hourly_void_count", int_bytes), 12)

    def test_mcp_schemas_and_tools_defined(self):
        """Verifies declarative MCP SQL tool definitions."""
        self.assertIn("read_cashier_realtime_alerts_sql", MCP_BIGTABLE_SQL_TOOLS)
        self.assertIn("read_pos_transactions_enriched_sql", MCP_BIGTABLE_SQL_TOOLS)

        tool_def = MCP_BIGTABLE_SQL_TOOLS["read_cashier_realtime_alerts_sql"]
        self.assertEqual(tool_def["name"], "read_cashier_realtime_alerts_sql")
        self.assertIn("store_id", tool_def["parameters"]["required"])
        self.assertIn("cashier_id", tool_def["parameters"]["required"])

    @patch.dict("os.environ", {"BIGTABLE_MCP_SERVICE_URL": ""})
    @patch("app.tools.bigtable_tool.bigtable")
    def test_read_cashier_realtime_alerts_direct_sdk_mock(self, mock_bt_module):
        """Verifies direct SDK fallback parsing."""
        mock_client = MagicMock()
        mock_bt_module.Client.return_value = mock_client
        mock_bt_module.row_set.RowSet.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_table = MagicMock()
        mock_instance.table.return_value = mock_table

        # Simulate row returned
        mock_row = MagicMock()
        mock_row.row_key = b"STORE_048#CASH_1190#99999"
        rate_bytes = struct.pack(">d", 0.35)
        void_bytes = struct.pack(">q", 8)
        mock_cell_rate = MagicMock()
        mock_cell_rate.value = rate_bytes
        mock_cell_void = MagicMock()
        mock_cell_void.value = void_bytes

        mock_row.cells = {
            "stats": {
                b"hourly_override_rate": [mock_cell_rate],
                b"hourly_void_count": [mock_cell_void],
            }
        }
        mock_table.read_rows.return_value = [mock_row]

        result = read_cashier_realtime_alerts("STORE_048", "CASH_1190")
        self.assertIn("Hourly Manual Override Rate: 0.35", result)
        self.assertIn("Hourly Void Count: 8", result)

    def test_declarative_mcp_sql_functions_callable(self):
        """Verifies exported MCP SQL interface functions."""
        with patch("app.tools.bigtable_tool.read_cashier_realtime_alerts") as mock_read:
            mock_read.return_value = "Cashier Telemetry Result"
            res1 = read_cashier_realtime_alerts_sql("STORE_048", "CASH_1190")
            res2 = read_pos_transactions_enriched_sql("STORE_048", "CASH_1190")
            self.assertEqual(res1, "Cashier Telemetry Result")
            self.assertEqual(res2, "Cashier Telemetry Result")


if __name__ == "__main__":
    unittest.main()
