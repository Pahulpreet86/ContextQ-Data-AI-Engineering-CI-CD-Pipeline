FROM public.ecr.aws/lambda/python:3.11

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Lambda function code
COPY app.py ${LAMBDA_TASK_ROOT}/app.py

# Set Lambda handler
CMD ["app.lambda_handler"]

