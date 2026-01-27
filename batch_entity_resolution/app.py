import json
import os
from typing import Any, Dict, List

from openai import OpenAI

client = OpenAI()


def _get_pairs_from_event(event: Any) -> List[Dict[str, Any]]:
    if event is None:
        return []

    # API Gateway / Lambda proxy
    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if isinstance(body, str) and body.strip():
            try:
                event = json.loads(body)
            except json.JSONDecodeError:
                return []

    if isinstance(event, list):
        return event

    if isinstance(event, dict) and isinstance(event.get("pairs"), list):
        return event["pairs"]

    return []


def lambda_handler(event, context):
    pairs = _get_pairs_from_event(event)

    items: List[Dict[str, Any]] = []
    for p in pairs:
        if not isinstance(p, dict):
            continue
        items.append(
            {
                "left_id": p.get("left_id"),
                "right_id": p.get("right_id"),
                "left_company_name": p.get("left_company_name", ""),
                "right_company_name": p.get("right_company_name", ""),
            }
        )

    if not items:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps([]),
        }

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

    schema = {
        "name": "company_name_matcher",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "results": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "left_id": {},
                            "right_id": {},
                            "match": {"type": "boolean"},
                            "match_confidence": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                            },
                        },
                        "required": ["left_id", "right_id", "match", "match_confidence"],
                    },
                }
            },
            "required": ["results"],
        },
    }

    system_msg = (
        "You are an entity resolution assistant.\n"
        "Decide whether left_company_name and right_company_name refer to the SAME real-world company.\n"
        "Return match=true only if you are confident they are the same company.\n"
        "match_confidence is a probability from 0 to 1.\n"
        "Return one result per input pair, copying left_id and right_id exactly."
    )

    user_payload = {"pairs": items}

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": json.dumps(user_payload)},
        ],
        response_format={"type": "json_schema", "json_schema": schema},
        temperature=0,
    )

    content = resp.choices[0].message.content or "{}"
    try:
        parsed = json.loads(content)
        results = parsed.get("results", [])
        if not isinstance(results, list):
            results = []
    except Exception:
        results = []

    returned_keys = set()
    for r in results:
        if isinstance(r, dict):
            returned_keys.add((r.get("left_id"), r.get("right_id")))

    for p in items:
        key = (p["left_id"], p["right_id"])
        if key not in returned_keys:
            results.append(
                {
                    "left_id": p["left_id"],
                    "right_id": p["right_id"],
                    "match": False,
                    "match_confidence": 0.0,
                }
            )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results),
    }

