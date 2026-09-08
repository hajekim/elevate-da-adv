"""POS Hardware Manual RAG Tool using BigQuery Vector Search.

Performs vector similarity search with adjacent context window stitching over
POS terminal runbooks in BigQuery (cymbal_gold.pos_manual_chunk_embeddings).
Enforces minimum similarity score thresholds (>= 0.70), full-text search fallback,
and certified refusal strings for out-of-domain queries.
"""

import logging
import os
import re
import time
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.70
CERTIFIED_REFUSAL_MSG = (
    "No certified POS terminal hardware troubleshooting runbook was found "
    "matching your specific query in official documentation. Please verify the "
    "hardware model and error code or escalate to Level 2 technical engineering."
)


def _convert_gcs_uri_to_https(uri: str) -> str:
    """Converts a gs:// URI to a clickable HTTPS console/storage URL."""
    if not uri:
        return ""
    if uri.startswith("gs://"):
        path = uri[5:]
        return f"https://storage.cloud.google.com/{path}"
    return uri


def pos_troubleshooting_rag_tool(query: str) -> str:
    """Vector similarity search with adjacent context window stitching over POS hardware manuals in BigQuery.

    Args:
        query: Specific hardware error code (e.g. ERR-PAY-4001, ERR-DN-PRNT-24V) or terminal maintenance SOP inquiry.

    Returns:
        Verified procedural runbook text with clickable HTTPS source documentation links,
        or a certified refusal message if relevance score is below 0.70.
    """
    project_id = os.getenv("PROJECT_ID", "elevate-da-adv-508004")
    dataset_id = "cymbal_gold"
    table_id = "pos_manual_chunk_embeddings"
    embedding_model = f"`{project_id}.module1_unstructureddata.pos_text_embedding_model`"

    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            client = bigquery.Client(project=project_id)

            # 1. Primary Vector Search with Adjacent Context Window Stitching (N-1 to N+1)
            vector_sql = f"""
            WITH query_emb AS (
                SELECT ml_generate_embedding_result AS emb
                FROM ML.GENERATE_EMBEDDING(
                    MODEL {embedding_model},
                    (SELECT @user_query AS content),
                    STRUCT('RETRIEVAL_QUERY' AS task_type)
                )
            ),
            matched_chunk AS (
                SELECT
                    base.document_filename,
                    base.document_title,
                    base.equipment_covered,
                    base.source_pdf_uri,
                    base.chunk_index,
                    ROUND(1.0 - distance, 4) AS similarity_score
                FROM VECTOR_SEARCH(
                    TABLE `{project_id}.{dataset_id}.{table_id}`,
                    'embedding',
                    TABLE query_emb,
                    top_k => 1,
                    distance_type => 'COSINE'
                )
            )
            SELECT
                m.document_filename,
                m.document_title,
                m.equipment_covered,
                m.source_pdf_uri,
                m.similarity_score,
                STRING_AGG(c.chunk_content, '\n' ORDER BY c.chunk_index ASC) AS stitched_procedure
            FROM matched_chunk m
            JOIN `{project_id}.{dataset_id}.{table_id}` c
                ON m.document_filename = c.document_filename
                AND c.chunk_index BETWEEN (m.chunk_index - 1) AND (m.chunk_index + 1)
            GROUP BY
                m.document_filename,
                m.document_title,
                m.equipment_covered,
                m.source_pdf_uri,
                m.similarity_score
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_query", "STRING", query)
                ]
            )

            results = list(client.query(vector_sql, job_config=job_config).result())

            if results:
                row = results[0]
                similarity = float(row.similarity_score or 0.0)

                # Strict quality gate: threshold >= 0.70
                if similarity >= SIMILARITY_THRESHOLD:
                    https_link = _convert_gcs_uri_to_https(row.source_pdf_uri)
                    return (
                        f"Document: {row.document_title} ({row.document_filename})\n"
                        f"Equipment: {row.equipment_covered}\n"
                        f"Similarity Score: {similarity:.4f}\n"
                        f"Source Documentation: {https_link}\n\n"
                        f"Certified Procedure:\n{row.stitched_procedure}"
                    )

            # 2. Fallback: Full-Text SEARCH query if vector similarity < 0.70 or no vector match
            fallback_sql = f"""
            SELECT
                document_filename,
                document_title,
                equipment_covered,
                source_pdf_uri,
                chunk_index,
                chunk_content
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE SEARCH(chunk_content, @user_query)
            ORDER BY chunk_index ASC
            LIMIT 3
            """

            fallback_results = list(client.query(fallback_sql, job_config=job_config).result())
            if fallback_results:
                row = fallback_results[0]
                https_link = _convert_gcs_uri_to_https(row.source_pdf_uri)
                return (
                    f"Document: {row.document_title} ({row.document_filename})\n"
                    f"Equipment: {row.equipment_covered}\n"
                    f"Retrieval Mode: Full-Text Fallback\n"
                    f"Source Documentation: {https_link}\n\n"
                    f"Procedure:\n{row.chunk_content}"
                )

            # 3. Out-of-Domain or uncertified query: Return certified refusal string verbatim
            return CERTIFIED_REFUSAL_MSG

        except Exception as e:
            logger.warning(
                "Error during POS RAG search (attempt %d): %s",
                attempt + 1,
                e,
            )
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))
            else:
                return CERTIFIED_REFUSAL_MSG

    return CERTIFIED_REFUSAL_MSG
