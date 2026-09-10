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

"""Integration tests for Cymbal Retail Operations Studio web endpoints."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from app.fast_api_app import app


class TestStudioEndpoints(unittest.TestCase):
    """Test suite for Operations Studio API and static files."""

    def setUp(self):
        self.client = TestClient(app)

    def test_root_serves_studio_html(self):
        """Verifies that / directly serves the Operations Studio HTML interface."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Cymbal Retail Operations Studio", response.text)

    def test_root_serves_app_js(self):
        """Verifies that /app.js directly serves the application script."""
        response = self.client.get("/app.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("SCENARIOS", response.text)

    def test_studio_static_html(self):
        """Verifies that /studio/ serves the English HTML studio interface."""
        response = self.client.get("/studio/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Cymbal Retail Operations Studio", response.text)
        self.assertIn("Operations Studio", response.text)
        self.assertIn("GoogleSQL", response.text)

    def test_studio_static_javascript(self):
        """Verifies that /studio/app.js serves the frontend application script."""
        response = self.client.get("/studio/app.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("SCENARIOS", response.text)
        self.assertIn("submitMessage", response.text)

    def test_studio_redirect(self):
        """Verifies that /studio redirects to /studio/."""
        response = self.client.get("/studio", follow_redirects=False)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers.get("location"), "/studio/")

    def test_create_new_session(self):
        """Verifies /api/sessions/new returns a formatted session ID."""
        response = self.client.post("/api/sessions/new")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("session_id", data)
        self.assertTrue(data["session_id"].startswith("sess_"))
        self.assertEqual(data.get("status"), "created")

    def test_studio_chat_endpoint_contract(self):
        """Verifies that /api/chat returns the complete structured payload contract."""
        # Mock runner event stream
        mock_runner = MagicMock()
        mock_part = MagicMock()
        mock_part.function_call = None
        mock_part.function_response = None
        mock_part.text = "Operational assessment complete. No critical anomalies detected."
        mock_event = MagicMock()
        mock_event.content.parts = [mock_part]

        async def mock_run_async(*args, **kwargs):
            yield mock_event

        mock_runner.run_async = mock_run_async
        app.state.runner = mock_runner

        response = self.client.post(
            "/api/chat",
            json={
                "message": "Check inventory health for store STORE_048",
                "session_id": "test_sess_001",
                "agent_override": "auto",
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["session_id"], "test_sess_001")
        self.assertIn("final_text", data)
        self.assertIn("dispatch_mode", data)
        self.assertIn("tool_calls", data)
        self.assertIn("tool_responses", data)
        self.assertIn("generated_sql", data)
        self.assertIn("sop_data", data)
        self.assertIn("gcs_links", data)
        self.assertIn("plotly_spec", data)
        self.assertIn("latency_ms", data)
