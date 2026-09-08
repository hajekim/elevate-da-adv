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

"""Unit and integration tests for Cymbal Retail Operations Agent coordinator."""

import unittest
from unittest.mock import MagicMock, patch

from app.agent import (
    app,
    on_turn_start_state_callback,
    root_agent,
    validate_and_update_temporal_cache,
)


class TestAgentCoordinator(unittest.TestCase):
    """Test suite for agent coordinator initialization and state orchestration."""

    def test_root_agent_initialization(self):
        """Verifies root agent name, instruction, and tool gateway binding."""
        self.assertEqual(root_agent.name, "cymbal_operations_agent")
        self.assertIn("cymbal_operations_agent", root_agent.instruction)
        self.assertEqual(len(root_agent.tools), 3)

        tool_names = [getattr(t, "name", t.__name__) for t in root_agent.tools]
        self.assertIn("cymbal_analytics_tool", tool_names)
        self.assertIn("pos_troubleshooting_rag_tool", tool_names)
        self.assertIn("read_cashier_realtime_alerts", tool_names)

    def test_app_container_initialization(self):
        """Verifies ADK App container configuration."""
        self.assertEqual(app.name, "cymbal_operations_agent")
        self.assertIsNotNone(app.root_agent)

    def test_temporal_cache_invalidation_same_day(self):
        """Verifies that cashier cache is preserved within the same calendar day."""
        session_state = {
            "top_offender_id": "CASH_1190",
            "top_offender_date": "2026-09-08",
        }
        updated = validate_and_update_temporal_cache(session_state, current_date_str="2026-09-08")
        self.assertEqual(updated["top_offender_id"], "CASH_1190")
        self.assertEqual(updated["top_offender_date"], "2026-09-08")

    def test_temporal_cache_invalidation_new_day(self):
        """Verifies that cashier cache is purged when a query occurs on a new calendar day."""
        session_state = {
            "top_offender_id": "CASH_1190",
            "top_offender_date": "2026-09-07",
        }
        updated = validate_and_update_temporal_cache(session_state, current_date_str="2026-09-08")
        self.assertIsNone(updated["top_offender_id"])
        self.assertEqual(updated["top_offender_date"], "2026-09-08")

    def test_temporal_cache_initialization(self):
        """Verifies initial state population when no date was previously cached."""
        session_state = {}
        updated = validate_and_update_temporal_cache(session_state, current_date_str="2026-09-08")
        self.assertEqual(updated["top_offender_date"], "2026-09-08")

    def test_turn_start_callback(self):
        """Verifies that the session turn callback properly executes cache validation."""
        mock_session = MagicMock()
        mock_session.state = {
            "top_offender_id": "CASH_0411",
            "top_offender_date": "2026-09-01",
        }
        on_turn_start_state_callback(mock_session)
        # Should have invalidated because date is old
        self.assertIsNone(mock_session.state.get("top_offender_id"))


if __name__ == "__main__":
    unittest.main()
