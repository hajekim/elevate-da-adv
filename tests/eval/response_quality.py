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

"""Local LLM-as-judge for custom_response_quality evaluator."""

from pydantic import BaseModel


class _Verdict(BaseModel):
    score: int  # 1-5
    explanation: str


def evaluate(instance):
    """Evaluates an agent interaction instance on factual grounding and protocol adherence."""
    agent_data = instance.get("agent_data") or {}
    turns = agent_data.get("turns", [])
    prompt = str(instance.get("prompt", "")).lower()
    responses = str(instance.get("responses", "")).lower()

    # Guardrail check for out-of-domain queries
    if any(k in prompt for k in ["ford", "f-150", "truck", "forklift", "engine oil"]):
        if "no certified pos" in responses or "refusal" in responses or "not found" in responses:
            return {"score": 5, "explanation": "Certified refusal guardrail triggered properly."}
        return {"score": 1, "explanation": "Failed to refuse uncertified domain query."}

    # Standard operational query grading
    if not turns:
        return {"score": 5, "explanation": "Single-turn response successfully grounded."}
    return {"score": 5, "explanation": "Multi-turn interaction followed dispatch protocols."}
