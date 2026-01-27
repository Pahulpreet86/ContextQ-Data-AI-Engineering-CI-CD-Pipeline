# Adverse News Lambda – Local Setup & Testing
Build the image

```
docker build -t company-match-lambda .
```
### Run the container locally
```
docker run --rm -p 9000:8080 \
  -e OPENAI_API_KEY="YOUR_KEY" \
  -e OPENAI_MODEL="gpt-4.1-mini" \
  company-match-lambda
```

The Lambda runtime listens on port 8080 inside the container, mapped to 9000 locally.

### Invoke the Lambda function
```
curl -s -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -H "Content-Type: application/json" \
  -d '{
    "pairs": [
      {"left_id":"38d1bd3dcfc1d21acbdb633554fa40f0040e938222d631f469361ea664c08f0f","right_id":"a6a4b3d211a23f68d181e726a9fe77117c94893d5759d90c4c0f853638a65a8e","left_company_name":"ibm","right_company_name":"international business machines"}
    ]
  }' | jq
```

### Response Format
```
[
  {
    "left_id": "38d1bd3dcfc1d21acbdb633554fa40f0040e938222d631f469361ea664c08f0f",
    "right_id": "a6a4b3d211a23f68d181e726a9fe77117c94893d5759d90c4c0f853638a65a8e",
    "match": true,
    "match_confidence": 0.95
  }
]
```