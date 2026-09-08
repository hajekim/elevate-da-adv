# Cymbal Retail Operations Agent Guidance

This document defines operational guidelines, dispatch protocols, and evaluation instructions for `cymbal_operations_agent`.

## Architecture Overview
The coordinator agent orchestrates three decoupled toolsets:
1. `cymbal_analytics_tool`: Natural language relational queries to BigQuery Conversational Data Agent (`global` location endpoint).
2. `pos_troubleshooting_rag_tool`: BigQuery Vector Search over `pos_manual_chunk_embeddings` with adjacent chunk stitching and 0.70 similarity threshold refusal.
3. `read_cashier_realtime_alerts`: Sub-second telemetry lookup from Cloud Bigtable `operations-db:cashier_realtime_alerts` using row key `STORE_<ID>#CASH_<ID>`.

## Dispatch Protocols
- Single-Tool Dispatch: Direct analytical, RAG, or telemetry inquiries.
- Parallel Dispatch: Comparing real-time telemetry against historical baselines (e.g. UC-2.2).
- Sequential Multi-Turn Dispatch: Cross-cloud audits starting from BigQuery anomaly tables to federated AWS S3 tables (e.g. UC-2.3).
- Strict Grounding: Every response sentence must trace directly to data returned by invoked tools.

## Evaluation Workflow
To run evaluation locally via `agents-cli`:
```bash
agents-cli eval run --dataset tests/eval/datasets/basic-dataset.json --metrics tool_use_quality,grounding
```
To evaluate against custom multi-turn datasets:
```bash
agents-cli eval run --dataset tests/eval/datasets/eval-multi-turn.json --eval-config tests/eval/eval_config.yaml
```

## Deployment Workflow
To deploy to Vertex AI Agent Runtime:
```bash
agents-cli deploy --target agent_runtime --service-account cymbal-sa-data@<PROJECT_ID>.iam.gserviceaccount.com
```
