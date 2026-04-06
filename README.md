# ML Streaming Pipeline — Terraform

A real-time ML inference pipeline on AWS. Streams events from Elasticsearch through Kinesis, enriches them with Apache Flink, runs ML predictions via a REST API, and stores results in DynamoDB.

---

## Architecture

```
Elasticsearch → Custom Producer → Kinesis (Raw)
                                      ↓
                              Kinesis Analytics (Flink)
                              + Redis lookup (zip, segments)
                                      ↓
                              Kinesis (Enriched)
                                      ↓
                                   Lambda
                                 ↙       ↘
                          ML REST API   SQS DLQ
                               ↓
                           DynamoDB

           CloudWatch monitors all components
```

---

## Prerequisites

| Tool | Version |
|---|---|
| Terraform | >= 1.5 |
| AWS CLI | >= 2.0 |
| Python | 3.12 (for Lambda) |
| Java / Scala | 11+ (for Flink JAR) |

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/your-org/ml-streaming-pipeline.git
cd ml-streaming-pipeline
```

Copy and edit the variables file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

```hcl
# terraform.tfvars
aws_region       = "us-east-1"
project_name     = "ml-streaming-pipeline"
environment      = "dev"
ml_api_endpoint  = "https://your-ml-api.example.com/predict"
ml_api_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789:secret:ml-api-key"
```

### 2. Build the Lambda package

```bash
mkdir package
pip install requests boto3 -t ./package/
cp handler.py ./package/
cd package && zip -r ../lambda_package.zip . && cd ..
```

### 3. Upload the Flink JAR

Build your Flink job JAR, then upload it:

```bash
# After terraform apply, the bucket name is printed as output
aws s3 cp target/flink-app.jar s3://<flink-bucket>/flink-app.jar
```

### 4. Deploy

```bash
terraform init       # download AWS provider plugin
terraform plan       # preview what will be created
terraform apply      # build everything (~5-10 min)
```

---

## Project Structure

```
.
├── main.tf              # all infrastructure resources
├── variables.tf         # input variable definitions
├── outputs.tf           # values printed after apply
├── terraform.tfvars     # your local config (git-ignored)
├── handler.py           # Lambda function code
├── lambda_package.zip   # built Lambda deployment package
└── README.md
```

---

## Resources Created

| Resource | Purpose |
|---|---|
| VPC + 2 private subnets | Isolated network for Redis, Flink, Lambda |
| Security groups (×3) | Firewall rules per service |
| Kinesis stream: raw | Ingest from Elasticsearch producer |
| Kinesis stream: enriched | Flink output → Lambda |
| ElastiCache Redis (×2 nodes) | Sub-ms lookup table for Flink |
| Kinesis Analytics (Flink) | Feature extraction, enrichment |
| S3 bucket | Flink JAR + checkpoints |
| Lambda function | Schema validation, ML API call, DynamoDB write |
| SQS DLQ | Failed record retention (14 days) |
| DynamoDB table | Prediction output storage |
| CloudWatch alarms (×5) | Error rate, lag, DLQ depth monitoring |
| CloudWatch dashboard | 6-panel live metrics view |

---

## Lambda Handler

Your `handler.py` must export a `lambda_handler` function:

```python
import os, json, boto3, requests

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

def lambda_handler(event, context):
    for record in event['Records']:
        # Decode Kinesis record
        payload = json.loads(
            base64.b64decode(record['kinesis']['data'])
        )

        # Call ML REST API
        response = requests.post(
            os.environ['ML_API_ENDPOINT'],
            json=payload,
            timeout=10
        )
        prediction = response.json()

        # Write to DynamoDB
        table.put_item(Item={
            'record_id': payload['id'],
            'timestamp': payload['timestamp'],
            'prediction': prediction['label'],
            'score': str(prediction['score']),
        })
```

---

## Flink Job

Your Flink JAR reads config from environment properties at runtime:

```java
ParameterTool params = ParameterTool.fromMap(
    env.getExecutionEnvironment()
       .getConfiguration()
       .toMap()
);

String inputStreamArn  = params.get("input.stream.arn");
String outputStreamArn = params.get("output.stream.arn");
String redisHost       = params.get("redis.host");
```

Features computed in Flink:
- Hour of day, day of week, session duration
- Zip-code → region lookup via Redis
- Behavioral tags from customer segment data

---

## Monitoring

After deploy, open the CloudWatch dashboard:

```bash
terraform output dashboard_url
```

### Alarms

| Alarm | Threshold | Meaning |
|---|---|---|
| Lambda errors | > 5 per minute | Processing failures |
| Raw stream lag | > 60 seconds | Producer faster than Flink |
| Enriched stream lag | > 60 seconds | Flink faster than Lambda |
| DLQ depth | > 10 messages | Records failing after retries |
| Flink downtime | > 0 seconds | App crashed |

---

## Teardown

To destroy all resources:

```bash
terraform destroy
```

> This deletes everything including DynamoDB data. For production, enable DynamoDB deletion protection first.

---

## Cost Estimate (us-east-1)

| Service | Est. monthly cost |
|---|---|
| Kinesis (2 streams × 2 shards) | ~$42 |
| ElastiCache Redis (2× t3.micro) | ~$25 |
| Lambda | ~$0–5 (depends on volume) |
| DynamoDB (on-demand) | ~$1–10 |
| CloudWatch | ~$4 |
| **Total** | **~$72–86/mo** |

---

## Security Notes

- All subnets are private — no public internet access
- Redis uses TLS in-transit + encryption at rest
- Lambda and Flink use least-privilege IAM roles
- SQS uses AWS-managed KMS encryption
- ML API credentials stored in Secrets Manager, not env vars in plaintext
