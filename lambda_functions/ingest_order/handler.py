"""Lambda: API Gateway -> validate order JSON -> Kinesis Data Streams.

Expects a POST with the order in the JSON request body (not the query string).
Authentication is enforced by API Gateway (JWT/Cognito or Lambda authorizer)
before this function runs.

Responses:
  202  accepted and written to Kinesis
  400  body is not valid JSON, or the order fails validation
  415  wrong content type
  500  Kinesis write failed (logged, safe to retry: order_id is the dedupe key)

Environment:
  STREAM_NAME  Kinesis data stream name

Deployment note: package this file together with src/validation.py.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from validation import validate_order

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_kinesis = None


def kinesis_client():
    global _kinesis
    if _kinesis is None:
        import boto3

        _kinesis = boto3.client("kinesis")
    return _kinesis


def response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    if "application/json" not in headers.get("content-type", ""):
        return response(415, {"error": "Content-Type must be application/json"})

    raw_body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        import base64

        raw_body = base64.b64decode(raw_body).decode("utf-8")

    try:
        order = json.loads(raw_body)
    except json.JSONDecodeError:
        return response(400, {"error": "request body is not valid JSON"})

    errors = validate_order(order)
    if errors:
        logger.info("Rejected order: %s", errors)
        return response(400, {"error": "validation failed", "details": errors})

    order.setdefault("event_timestamp", datetime.now(UTC).isoformat())
    order["ingested_at"] = datetime.now(UTC).isoformat()

    try:
        result = kinesis_client().put_record(
            StreamName=os.environ["STREAM_NAME"],
            Data=(json.dumps(order) + "\n").encode("utf-8"),
            PartitionKey=str(order["order_id"]),
        )
    except Exception:
        logger.exception("Failed to write order %s to Kinesis", order.get("order_id"))
        return response(500, {"error": "could not accept order, please retry"})

    return response(
        202,
        {
            "message": "order accepted",
            "order_id": order["order_id"],
            "sequence_number": result.get("SequenceNumber"),
        },
    )
