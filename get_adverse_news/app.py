import json
import os
from typing import Any, Dict
from datetime import datetime

from openai import OpenAI

client = OpenAI()

JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "corporate_id": {"type": "string"},
        "company": {"type": "string"},
        "current_date": {"type": "string"},
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "date": {"type": "string"},
                    "title": {"type": "string"},
                    "url": {"type": "string"},
                    # Optional but useful for joins; keep if you want item-level denormalization
                    "corporate_id": {"type": "string"},
                },
                "required": ["date", "title", "url", "corporate_id"],
            },
        },
    },
    "required": ["corporate_id", "company", "current_date", "items"],
}


def _parse_params(event: Dict[str, Any]) -> Dict[str, str]:
    body = {}

    if isinstance(event, dict):
        # API Gateway / Lambda URL
        if event.get("body"):
            try:
                body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
            except Exception:
                body = {}
        else:
            # Direct invoke
            body = event

        # Querystring support
        qsp = event.get("queryStringParameters") or {}
        for k in ("corporate_id", "company", "current_date"):
            if not body.get(k) and qsp.get(k):
                body[k] = qsp.get(k)

    return {
        "corporate_id": str(body.get("corporate_id", "")).strip(),
        "company": str(body.get("company", "")).strip(),
        "current_date": str(body.get("current_date", "")).strip(),
    }


def _validate_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    params = _parse_params(event)
    corporate_id = params["corporate_id"]
    company = params["company"]
    current_date = params["current_date"]

    if not corporate_id:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Missing required parameter: corporate_id"}),
        }

    if not company:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Missing required parameter: company"}),
        }

    if not current_date or not _validate_date(current_date):
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Missing or invalid required parameter: current_date (YYYY-MM-DD)"}),
        }

    prompt = f"""
You are a risk analyst.

Task:
Find credible adverse / negative news about the company "{company}" (corporate_id: {corporate_id}).

STRICT RULES:
- ONLY include news with a publication date >= {current_date}
- EXCLUDE anything before {current_date}
- Adverse news includes: lawsuits, regulatory actions, fines, sanctions, fraud allegations,
  major security incidents, bankruptcies, major layoffs, recalls, or serious controversies.
- Prefer reputable sources (major media, regulators, courts).
- If no qualifying news exists, return an empty items array.

Output:
- Must strictly match the provided JSON schema
- Dates must be in YYYY-MM-DD format (best effort)
- Return up to 10 items
- Include corporate_id in every item
"""

    try:
        response = client.responses.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-5.2"),
            input=prompt,
            tools=[{"type": "web_search"}],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "adverse_news",
                    "schema": JSON_SCHEMA,
                    "strict": True,
                }
            },
            max_output_tokens=1200,
        )

        result = json.loads(response.output_text)

        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(result),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Internal error", "details": str(e)}),
        }
