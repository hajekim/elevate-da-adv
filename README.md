# Cymbal Retail Operations Agent (`cymbal_operations_agent`)

[![Track](https://img.shields.io/badge/Project_Elevate-Data_Analytics_Advanced-blue.svg)](https://github.com/google)
[![Runtime](https://img.shields.io/badge/Target-Vertex_AI_Agent_Runtime-green.svg)](https://cloud.google.com/vertex-ai)
[![Eval](https://img.shields.io/badge/Evaluation-agents--cli-purple.svg)](https://github.com/google/agents-cli)

Coordinator AI Agent and Evaluation Pipeline for Cymbal Retail store leads, technicians, and loss-prevention auditors.

---

## 📦 Prescribed Repository Structure

```text
elevate-da-adv/
├── app/                              # Core agent implementation
│   ├── __init__.py
│   ├── agent.py                      # Root coordinator & BigQuery telemetry
│   └── tools/
│       ├── __init__.py
│       ├── analytics_tool.py         # BigQuery Data Agent wrapper
│       ├── rag_tool.py               # Vector Search RAG with 0.70 refusal
│       └── bigtable_tool.py          # Bigtable sub-second telemetry
├── tests/
│   └── eval/                         # Evaluation pipeline
│       ├── datasets/                 # JSON evaluation datasets
│       │   ├── basic-dataset.json    # Golden benchmark dataset
│       │   ├── eval-data.json        # Single-turn BRD scenarios
│       │   └── eval-multi-turn.json  # Multi-turn context & guardrail scenarios
│       ├── eval_config.yaml          # Metrics and scoring configuration
│       ├── evaluation_report.md      # 4-domain comprehensive evaluation report
│       └── response_quality.py       # Custom evaluator implementation
├── agents-cli-manifest.yaml          # Agent Runtime deployment manifest
├── pyproject.toml                    # Package dependencies and build config
├── AGENTS.md                         # Agent operational guidance
└── SDD.md                            # Hardened Solution Design Document (v1.1)
```

---

## 🚀 Quickstart & Local Evaluation

1. **Install Dependencies:**
   ```bash
   uv venv && source .venv/bin/activate
   pip install -e .
   ```

2. **Configure Environment Variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your assigned GCP Project ID
   ```

3. **Run Evaluation Suite:**
   ```bash
   # Run baseline evaluation
   agents-cli eval run --dataset tests/eval/datasets/basic-dataset.json --metrics tool_use_quality,grounding

   # Run multi-turn and guardrail evaluation
   agents-cli eval run --dataset tests/eval/datasets/eval-multi-turn.json --eval-config tests/eval/eval_config.yaml
   ```

4. **Deploy to Vertex AI Agent Runtime:**
   ```bash
   agents-cli deploy --target agent_runtime --region us-central1
   ```
