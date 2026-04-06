# ─────────────────────────────────────────────────────────────────────────────
# Architecture:
#   Elasticsearch → Custom Producer
#     → Kinesis (Raw) → Kinesis Data Analytics (Flink)
#       → Redis (Lookup) [used during Flink processing]
#     → Kinesis (Enriched) → Lambda
#       → ML REST API → DynamoDB
#       └─ Failure → SQS DLQ
#   CloudWatch monitors everything
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Variables ──────────────────────────────────────────────────────────────

variable "aws_region"        { default = "us-east-1" }
variable "project_name"      { default = "ml-streaming-pipeline" }
variable "environment"       { default = "dev" }
variable "ml_api_endpoint"   { default = "https://your-ml-api.example.com/predict" }
variable "ml_api_secret_arn" { default = "" }

locals {
  prefix = "${var.project_name}-${var.environment}"
}

data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" { state = "available" }

# ── VPC (Lambda + ElastiCache + Flink) ────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "${local.prefix}-vpc" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags = { Name = "${local.prefix}-private-${count.index}" }
}

resource "aws_security_group" "lambda" {
  name   = "${local.prefix}-lambda-sg"
  vpc_id = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "flink" {
  name   = "${local.prefix}-flink-sg"
  vpc_id = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "redis" {
  name   = "${local.prefix}-redis-sg"
  vpc_id = aws_vpc.main.id
  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.flink.id, aws_security_group.lambda.id]
  }
}

# ── S3 bucket for Flink app JAR + checkpoints ─────────────────────────────

resource "aws_s3_bucket" "flink" {
  bucket        = "${local.prefix}-flink-artifacts"
  force_destroy = true
  tags          = { Env = var.environment }
}

resource "aws_s3_bucket_public_access_block" "flink" {
  bucket                  = aws_s3_bucket.flink.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Kinesis Stream 1: Raw (Elasticsearch → Flink) ─────────────────────────

resource "aws_kinesis_stream" "raw" {
  name             = "${local.prefix}-raw-stream"
  shard_count      = 2
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { Role = "raw-ingest", Env = var.environment }
}

resource "aws_kinesis_stream_consumer" "flink_efo" {
  name       = "${local.prefix}-flink-consumer"
  stream_arn = aws_kinesis_stream.raw.arn
}

# ── Kinesis Stream 2: Enriched (Flink → Lambda) ───────────────────────────

resource "aws_kinesis_stream" "enriched" {
  name             = "${local.prefix}-enriched-stream"
  shard_count      = 2
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { Role = "enriched-output", Env = var.environment }
}

resource "aws_kinesis_stream_consumer" "lambda_efo" {
  name       = "${local.prefix}-lambda-consumer"
  stream_arn = aws_kinesis_stream.enriched.arn
}

# ── ElastiCache Redis (Lookup for Flink) ──────────────────────────────────

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.prefix}-redis-subnet"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${local.prefix}-redis"
  description                = "Lookup cache for Flink (zip-code, segments)"
  node_type                  = "cache.t3.micro"
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.redis.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  tags = { Env = var.environment }
}

# ── IAM: Kinesis Data Analytics (Flink) ───────────────────────────────────

resource "aws_iam_role" "flink" {
  name = "${local.prefix}-flink-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "flink" {
  name = "${local.prefix}-flink-policy"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords", "kinesis:GetShardIterator",
          "kinesis:DescribeStream", "kinesis:ListShards",
          "kinesis:SubscribeToShard", "kinesis:PutRecord", "kinesis:PutRecords"
        ]
        Resource = [
          aws_kinesis_stream.raw.arn,
          aws_kinesis_stream_consumer.flink_efo.arn,
          aws_kinesis_stream.enriched.arn
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.flink.arn, "${aws_s3_bucket.flink.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:PutLogEvents", "logs:CreateLogGroup", "logs:CreateLogStream"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface", "ec2:DescribeVpcs",
                    "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"]
        Resource = "*"
      }
    ]
  })
}

# ── Kinesis Data Analytics Application (Flink) ────────────────────────────

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${local.prefix}-flink"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink-app-stream"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = "${local.prefix}-flink-app"
  runtime_environment    = "FLINK-1_18"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.flink.arn
          file_key   = "flink-app.jar" # upload your Flink JAR here
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "CUSTOM"
        checkpointing_enabled = true
        checkpoint_interval   = 60000  # 60 seconds
        min_pause_between_checkpoints = 5000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }
      parallelism_configuration {
        configuration_type = "CUSTOM"
        auto_scaling_enabled = true
        parallelism          = 2
        parallelism_per_kpu  = 1
      }
    }

    environment_properties {
      property_group {
        property_group_id = "AppConfig"
        property_map = {
          "input.stream.arn"   = aws_kinesis_stream.raw.arn
          "output.stream.arn"  = aws_kinesis_stream.enriched.arn
          "redis.host"         = aws_elasticache_replication_group.redis.primary_endpoint_address
          "redis.port"         = "6379"
          "aws.region"         = var.aws_region
        }
      }
    }

    vpc_configuration {
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.flink.id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = { Env = var.environment }
}

# ── DynamoDB Table (Predictions output) ───────────────────────────────────

resource "aws_dynamodb_table" "predictions" {
  name         = "${local.prefix}-predictions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "record_id"
  range_key    = "timestamp"

  attribute {
    name = "record_id"
    type = "S"
  }
  attribute {
    name = "timestamp"
    type = "S"
  }

  # GSI to query by session or user
  global_secondary_index {
    name            = "session-index"
    hash_key        = "timestamp"
    range_key       = "record_id"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Env = var.environment }
}

# ── SQS Dead Letter Queue ──────────────────────────────────────────────────

resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-dlq"
  message_retention_seconds = 1209600 # 14 days
  kms_master_key_id         = "alias/aws/sqs"
  tags                      = { Env = var.environment }
}

# ── IAM: Lambda ───────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda" {
  name = "${local.prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_extras" {
  name = "${local.prefix}-lambda-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator",
                    "kinesis:DescribeStream", "kinesis:ListShards",
                    "kinesis:SubscribeToShard"]
        Resource = [aws_kinesis_stream.enriched.arn,
                    aws_kinesis_stream_consumer.lambda_efo.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:BatchWriteItem"]
        Resource = aws_dynamodb_table.predictions.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.dlq.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.ml_api_secret_arn != "" ? [var.ml_api_secret_arn] : ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

# ── Lambda Function ────────────────────────────────────────────────────────

resource "aws_lambda_function" "processor" {
  function_name = "${local.prefix}-processor"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.12"
  handler       = "handler.lambda_handler"
  timeout       = 60
  memory_size   = 512
  filename      = "lambda_package.zip" # built from handler.py + dependencies

  environment {
    variables = {
      ML_API_ENDPOINT  = var.ml_api_endpoint
      ML_SECRET_ARN    = var.ml_api_secret_arn
      DYNAMODB_TABLE   = aws_dynamodb_table.predictions.name
      DLQ_URL          = aws_sqs_queue.dlq.id
      AWS_REGION_NAME  = var.aws_region
    }
  }

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  tags = { Env = var.environment }
}

# ── Kinesis (Enriched) → Lambda Event Source Mapping (EFO) ───────────────

resource "aws_lambda_event_source_mapping" "enriched_kinesis" {
  event_source_arn              = aws_kinesis_stream_consumer.lambda_efo.arn
  function_name                 = aws_lambda_function.processor.arn
  starting_position             = "LATEST"
  batch_size                    = 100
  parallelization_factor        = 2
  bisect_batch_on_function_error = true
  maximum_retry_attempts        = 3
  maximum_record_age_in_seconds = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }
}

# ── CloudWatch Alarms ──────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.prefix}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  dimensions          = { FunctionName = aws_lambda_function.processor.function_name }
}

resource "aws_cloudwatch_metric_alarm" "raw_iterator_age" {
  alarm_name          = "${local.prefix}-raw-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60000
  dimensions          = { StreamName = aws_kinesis_stream.raw.name }
}

resource "aws_cloudwatch_metric_alarm" "enriched_iterator_age" {
  alarm_name          = "${local.prefix}-enriched-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60000
  dimensions          = { StreamName = aws_kinesis_stream.enriched.name }
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${local.prefix}-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
}

resource "aws_cloudwatch_metric_alarm" "flink_downtime" {
  alarm_name          = "${local.prefix}-flink-downtime"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "downtime"
  namespace           = "AWS/KinesisAnalytics"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  dimensions          = { Application = aws_kinesisanalyticsv2_application.flink.name }
}

resource "aws_cloudwatch_metric_alarm" "dynamodb_errors" {
  alarm_name          = "${local.prefix}-dynamodb-write-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "SystemErrors"
  namespace           = "AWS/DynamoDB"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  dimensions          = { TableName = aws_dynamodb_table.predictions.name }
}

# ── CloudWatch Dashboard ───────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${local.prefix}-dashboard"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 8, height = 6,
        properties = {
          title   = "Raw stream iterator age"
          metrics = [["AWS/Kinesis","GetRecords.IteratorAgeMilliseconds","StreamName",aws_kinesis_stream.raw.name]]
          period = 60, stat = "Maximum", view = "timeSeries"
        }
      },
      {
        type = "metric", x = 8, y = 0, width = 8, height = 6,
        properties = {
          title   = "Flink KPU utilization"
          metrics = [["AWS/KinesisAnalytics","KPUs","Application",aws_kinesisanalyticsv2_application.flink.name]]
          period = 60, stat = "Average", view = "timeSeries"
        }
      },
      {
        type = "metric", x = 16, y = 0, width = 8, height = 6,
        properties = {
          title   = "Lambda errors"
          metrics = [["AWS/Lambda","Errors","FunctionName",aws_lambda_function.processor.function_name]]
          period = 60, stat = "Sum", view = "timeSeries"
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 8, height = 6,
        properties = {
          title   = "DynamoDB write latency"
          metrics = [["AWS/DynamoDB","SuccessfulRequestLatency","TableName",aws_dynamodb_table.predictions.name,"Operation","PutItem"]]
          period = 60, stat = "p99", view = "timeSeries"
        }
      },
      {
        type = "metric", x = 8, y = 6, width = 8, height = 6,
        properties = {
          title   = "SQS DLQ depth"
          metrics = [["AWS/SQS","ApproximateNumberOfMessagesVisible","QueueName",aws_sqs_queue.dlq.name]]
          period = 300, stat = "Sum", view = "timeSeries"
        }
      },
      {
        type = "metric", x = 16, y = 6, width = 8, height = 6,
        properties = {
          title   = "Enriched stream iterator age"
          metrics = [["AWS/Kinesis","GetRecords.IteratorAgeMilliseconds","StreamName",aws_kinesis_stream.enriched.name]]
          period = 60, stat = "Maximum", view = "timeSeries"
        }
      }
    ]
  })
}

# ── Outputs ────────────────────────────────────────────────────────────────

output "raw_stream_arn"       { value = aws_kinesis_stream.raw.arn }
output "enriched_stream_arn"  { value = aws_kinesis_stream.enriched.arn }
output "flink_app_name"       { value = aws_kinesisanalyticsv2_application.flink.name }
output "redis_endpoint"       { value = aws_elasticache_replication_group.redis.primary_endpoint_address }
output "lambda_function_arn"  { value = aws_lambda_function.processor.arn }
output "dynamodb_table_name"  { value = aws_dynamodb_table.predictions.name }
output "dlq_url"              { value = aws_sqs_queue.dlq.id }
output "flink_s3_bucket"      { value = aws_s3_bucket.flink.bucket }
output "dashboard_url" {
  value = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}
