# Adverse News Lambda – Local Setup & Testing
Build the image

```
docker build -t adverse-news-lambda .
```
### Run the container locally
```
docker run -p 9000:8080 \
  -e OPENAI_API_KEY=sk-xxxx \
  -e OPENAI_MODEL=gpt-5.2 \
  adverse-news-lambda
```

The Lambda runtime listens on port 8080 inside the container, mapped to 9000 locally.

### Invoke the Lambda function
```
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -H "Content-Type: application/json" \
  -d '{
    "corporate_id": "C-000123",
    "company": "Tesla",
    "current_date": "2026-01-01"
  }'
```

### Response Format
```
{
  "statusCode": 200,
  "headers": {
    "content-type": "application/json"
  },
  "body": {
    "corporate_id": "072e28aa06992247a0f6558369d71166d8de4ce6114770af46c388d0fa4ef2f0",
    "company": "Tesla",
    "current_date": "2026-01-27",
    "items": [
      {
        "date": "2026-01-16",
        "title": "Tesla granted more time in US investigation into its self-driving tech (NHTSA probe of alleged traffic-law violations while using FSD)",
        "url": "https://apnews.com/article/tesla-elon-musk-full-selfdriving-investigation-61d7065b8ed2c805385b1f6731edcf81",
        "corporate_id": "072e28aa06992247a0f6558369d71166d8de4ce6114770af46c388d0fa4ef2f0"
      },
      {
        "date": "2026-01-07",
        "title": "California threatens Tesla with 30-day suspension of sales license for deceptive self-driving claims (state DMV action re Autopilot/FSD marketing)",
        "url": "https://apnews.com/article/tesla-self-driving-autopilot-deceptive-practices-b345d895e5e5e36dc76b4d3acd49f8b6",
        "corporate_id": "072e28aa06992247a0f6558369d71166d8de4ce6114770af46c388d0fa4ef2f0"
      }
    ]
  }
}
```