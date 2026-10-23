import json

import pytest

import handler


class FakeKinesis:
    def __init__(self, fail=False):
        self.fail = fail
        self.records = []

    def put_record(self, **kwargs):
        if self.fail:
            raise RuntimeError("kinesis is down")
        self.records.append(kwargs)
        return {"SequenceNumber": "123", "ShardId": "shardId-000000000000"}


@pytest.fixture
def kinesis(monkeypatch):
    fake = FakeKinesis()
    monkeypatch.setattr(handler, "_kinesis", fake)
    monkeypatch.setenv("STREAM_NAME", "sales-orders-stream")
    return fake


def api_event(body, content_type="application/json"):
    return {
        "headers": {"Content-Type": content_type},
        "body": body if isinstance(body, str) else json.dumps(body),
        "isBase64Encoded": False,
    }


ORDER = {
    "order_id": "ORD99001",
    "order_date": "2026-10-12",
    "product": "Monitor",
    "city": "Delhi",
    "quantity": 2,
    "unit_price": 12000,
    "payment_method": "Card",
}


def test_valid_order_goes_to_kinesis(kinesis):
    result = handler.lambda_handler(api_event(ORDER), None)

    assert result["statusCode"] == 202
    assert len(kinesis.records) == 1
    sent = kinesis.records[0]
    assert sent["StreamName"] == "sales-orders-stream"
    assert sent["PartitionKey"] == "ORD99001"
    payload = json.loads(sent["Data"])
    assert payload["order_id"] == "ORD99001"
    assert "ingested_at" in payload


def test_invalid_json_returns_400(kinesis):
    result = handler.lambda_handler(api_event("{not json"), None)
    assert result["statusCode"] == 400
    assert kinesis.records == []


def test_validation_errors_return_400_with_details(kinesis):
    result = handler.lambda_handler(api_event({**ORDER, "quantity": -5}), None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 400
    assert "quantity must be a positive whole number" in body["details"]
    assert kinesis.records == []


def test_wrong_content_type_returns_415(kinesis):
    result = handler.lambda_handler(api_event(ORDER, content_type="text/plain"), None)
    assert result["statusCode"] == 415


def test_kinesis_failure_returns_500(monkeypatch):
    monkeypatch.setattr(handler, "_kinesis", FakeKinesis(fail=True))
    monkeypatch.setenv("STREAM_NAME", "sales-orders-stream")
    result = handler.lambda_handler(api_event(ORDER), None)
    assert result["statusCode"] == 500
