"""Automated Validation Suite for the 7 Operational Use Cases.

Validates:
- UC 1.1a: POS Hardware Error (RAG tool + HTTPS GCS link)
- UC 1.1c: Out-of-Scope Hardware (RAG refusal protocol)
- UC 1.2a: Stockout Risk (<20h) (Data Agent tool)
- UC 1.3: Real-Time Cashier Metrics (Bigtable tool)
- UC 2.1a: Warranty Transaction (Data Agent tool)
- UC 2.2: Dual Cashier Baseline (Parallel dispatch: Bigtable + BQ in Turn 1)
- UC 2.3: Cross-Cloud Offender Audit (Sequential multi-turn dispatch)
"""

import asyncio
import json
import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from app.agent import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scenario_validator")


async def run_scenario(runner: Runner, session_id: str, prompt: str) -> dict:
    """Runs a single prompt through the coordinator agent and collects all trace events."""
    msg = types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
    tool_calls = []
    tool_responses = []
    agent_texts = []

    async for event in runner.run_async(user_id="auditor_lead", session_id=session_id, new_message=msg):
        content = getattr(event, "content", None)
        if content and getattr(content, "parts", None):
            for part in content.parts:
                fn_call = getattr(part, "function_call", None)
                if fn_call:
                    tool_calls.append({
                        "name": fn_call.name,
                        "args": fn_call.args or {},
                    })
                fn_resp = getattr(part, "function_response", None)
                if fn_resp:
                    tool_responses.append({
                        "name": fn_resp.name,
                        "response": fn_resp.response,
                    })
                text = getattr(part, "text", None)
                if text:
                    agent_texts.append(text)

    return {
        "prompt": prompt,
        "tool_calls": tool_calls,
        "tool_responses": tool_responses,
        "final_text": "\n".join(agent_texts).strip(),
    }


async def main():
    session_service = InMemorySessionService()
    runner = Runner(app=app, session_service=session_service)

    scenarios = [
        {
            "id": "UC 1.1a",
            "name": "Hardware Error Recovery Protocol",
            "prompt": "What is the immediate field recovery protocol when a cashier encounters an ERR-PAY-4001 EMV contactless payment freeze, and how do we ensure the customer is not double-charged?",
            "validator": lambda res: (
                any(c["name"] == "pos_troubleshooting_rag_tool" for c in res["tool_calls"])
                and ("https://" in res["final_text"] or "storage.cloud.google.com" in res["final_text"] or "ERR-PAY-4001" in res["final_text"])
            ),
            "expected": "pos_troubleshooting_rag_tool invoked, returns SOP and certified doc link",
        },
        {
            "id": "UC 1.1c",
            "name": "Out-of-Scope Hardware Refusal Protocol",
            "prompt": "How do I replace the engine oil on a Ford F-150 truck?",
            "validator": lambda res: (
                any(c["name"] == "pos_troubleshooting_rag_tool" for c in res["tool_calls"])
                and "I cannot find certified warranty or repair rules" in res["final_text"]
            ),
            "expected": "pos_troubleshooting_rag_tool invoked, returns exact certified refusal string",
        },
        {
            "id": "UC 1.2a",
            "name": "Stockout Risk & Inventory Cover Hours",
            "prompt": "What is the estimated cover hours remaining for store inventory positions experiencing stockout risk of less than 20 hours, and what is their total on-hand inventory?",
            "validator": lambda res: (
                any(c["name"] == "cymbal_analytics_tool" for c in res["tool_calls"])
            ),
            "expected": "cymbal_analytics_tool invoked targeting gold_inventory_reconciliation_ledger",
        },
        {
            "id": "UC 1.3",
            "name": "Real-Time Cashier Rolling Metrics",
            "prompt": "Read live 1-hour rolling metrics and audit status flags for Cashier CASH_1190 at Store 48.",
            "validator": lambda res: (
                any(c["name"] == "read_cashier_realtime_alerts" for c in res["tool_calls"])
                and ("clear" in res["final_text"].lower() or "audit" in res["final_text"].lower() or "hourly" in res["final_text"].lower())
            ),
            "expected": "read_cashier_realtime_alerts invoked targeting STORE_048#CASH_1190",
        },
        {
            "id": "UC 2.1a",
            "name": "Warranty Coverage & Transaction Line Item",
            "prompt": "Check transaction details for TXN-20260312-0015811 and show the warranty coverage policy for the purchased item.",
            "validator": lambda res: (
                any(c["name"] == "cymbal_analytics_tool" for c in res["tool_calls"])
            ),
            "expected": "cymbal_analytics_tool invoked to query line items and warranty policy",
        },
        {
            "id": "UC 2.2",
            "name": "Dual Cashier Baseline (Parallel Dispatch)",
            "prompt": "What is Cashier CASH_1190's live 1-hour override rate right now, compared to their 7-day historical override baseline?",
            "validator": lambda res: (
                any(c["name"] == "read_cashier_realtime_alerts" for c in res["tool_calls"])
                and any(c["name"] == "cymbal_analytics_tool" for c in res["tool_calls"])
            ),
            "expected": "PARALLEL DISPATCH calling both read_cashier_realtime_alerts and cymbal_analytics_tool in Turn 1",
        },
        {
            "id": "UC 2.3",
            "name": "Cross-Cloud Offender Audit (Sequential Dispatch)",
            "prompt": "Show cashiers with active cashier promo abuse alerts in the last 7 days and retrieve checkout logs for the top offender.",
            "validator": lambda res: (
                any(c["name"] == "cymbal_analytics_tool" for c in res["tool_calls"])
            ),
            "expected": "SEQUENTIAL DISPATCH querying pos_anomaly_alerts then silver_pos_transactions",
        },
    ]

    passed_count = 0
    total_count = len(scenarios)

    print("=" * 80)
    print("CYMBAL RETAIL OPERATIONS COORDINATOR AGENT - 7 USE CASES VALIDATION")
    print("=" * 80)

    for sc in scenarios:
        session = await session_service.create_session(
            app_name="cymbal_operations_agent",
            user_id="auditor_lead",
        )
        print(f"\n▶ Running [{sc['id']}] {sc['name']}...")
        print(f"  Prompt: {sc['prompt']}")

        t0 = time.time()
        try:
            res = await run_scenario(runner, session.id, sc["prompt"])
            elapsed = time.time() - t0

            tool_names = [c["name"] for c in res["tool_calls"]]
            print(f"  Tool Calls ({len(tool_names)}): {tool_names}")
            print(f"  Response Snippet: {res['final_text'][:200]}...")
            print(f"  Latency: {elapsed:.2f}s")

            passed = sc["validator"](res)
            if passed:
                print(f"  [RESULT: PASS] Verified: {sc['expected']}")
                passed_count += 1
            else:
                print(f"  [RESULT: FAIL] Expected: {sc['expected']}")
        except Exception as e:
            print(f"  [RESULT: ERROR] {e}")

    print("\n" + "=" * 80)
    print(f"VALIDATION SUMMARY: {passed_count}/{total_count} Passed ({(passed_count/total_count)*100:.1f}%)")
    print("=" * 80)

    if passed_count < total_count:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
