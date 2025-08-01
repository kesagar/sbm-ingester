# 📘 SBM Ingestion Infrastructure (Terraform)

This repository provisions the **SBM Ingestion pipeline** using Terraform on AWS.  
The system is **event-driven**, using **S3 → SQS → Lambda → API Gateway** with comprehensive logging and monitoring.

---

## 📐 Architecture Overview

The ingestion pipeline is designed as follows:

1. **S3 Event Notifications**  
   - Files uploaded into the bucket `sbm-file-ingester` under the prefix `newTBP/` trigger S3 notifications.  
   - Notifications are sent to an **SQS queue** `sbm-files-ingester-queue`.  

2. **Lambda Processing**  
   - The `sbm-files-ingester` Lambda consumes messages from SQS and processes the files. After processing, goes to hudibucketsrc as before.
   - Errors or failures are handled by the `sbm-files-ingester-redrive` Lambda.  

3. **Scheduled Processing**  
   - The `sbm-files-ingester-nem12-mappings-to-s3` Lambda runs hourly using a CloudWatch Event Rule.  
   - This Lambda is also integrated with API Gateway to expose the `/nem12-mappings` endpoint.  

4. **API Gateway Integration**  
   - API Gateway exposes a GET endpoint at `/nem12-mappings`.  
   - Secured with an API key and a usage plan limiting requests to 500/day.  

5. **Logging & Monitoring**  
   - All Lambdas have dedicated CloudWatch log groups with **30-day retention**.  
   - Custom log groups exist for execution, errors, metrics, parse errors, and runtime errors.  
   - API Gateway and S3 logging are also recommended.  

---

## 🔧 Terraform Resources

### 1. S3 Buckets
- **`sbm-file-ingester`**  
  - Holds raw ingestion files.  
  - Configured with event notifications to SQS.  
  - Should be configured with versioning, default encryption (SSE-S3 or KMS), and access logging.  

- **`gega-code-deployment-bucket`**  
  - Stores Lambda deployment artifacts (`ingester.zip`, `redrive.zip`, `nem12-mappings-to-s3.zip`).  
  - Should enforce versioning and lifecycle rules to clean up old builds.  

---

### 2. SQS Queue
- **`sbm-files-ingester-queue`**  
  - Buffers S3 events before Lambda processing.  
  - Visibility timeout: 300 seconds.  
  - Access restricted to S3 and Lambda.  

---

### 3. Lambda Functions
- **`sbm-files-ingester`**  
  - Runtime: Python 3.12  
  - Timeout: 120 seconds  
  - Reserved concurrency: 5  
  - Source: `gega-code-deployment-bucket/sbm-files-ingester/ingester.zip`  

- **`sbm-files-ingester-redrive`**  
  - Runtime: Python 3.12  
  - Timeout: 600 seconds  
  - Source: `gega-code-deployment-bucket/sbm-files-ingester/redrive.zip`  

- **`sbm-files-ingester-nem12-mappings-to-s3`**  
  - Runtime: Python 3.9  
  - Timeout: 60 seconds  
  - Source: `gega-code-deployment-bucket/sbm-files-ingester/nem12-mappings-to-s3.zip`  
  - VPC attached (for Neptune access).  
  - Scheduled hourly via CloudWatch Event Rule.  
  - API Gateway integration on `/nem12-mappings`.  

---

### 4. Logging & Monitoring

**Lambda Logging**
- `/aws/lambda/sbm-files-ingester` (default)  
- `/aws/lambda/sbm-files-ingester-redrive` (default)  
- `/aws/lambda/sbm-files-ingester-nem12-mappings-to-s3` (default)  
- Retention: **30 days**  

**Custom Log Groups**
- `sbm-ingester-error-log`  
- `sbm-ingester-execution-log`  
- `sbm-ingester-metrics-log`  
- `sbm-ingester-parse-error-log`  
- `sbm-ingester-runtime-error-log`  
- Retention: **30 days**  

**API Gateway Logging**
- Execution logs and metrics should be enabled at the `prod` stage.  

**S3 Access Logs**
- Should be configured to deliver to a dedicated log bucket.  

---

### 5. API Gateway
- REST API: `sbm-files-ingester-api`  
- Resource: `/nem12-mappings`  
- Method: `GET`  
- Security:  
  - API Key required - Get it from API Gateway (sbm-ingester-api-key)
  - Usage plan allows 500 requests/day  

---

### 6. IAM
- Uses existing IAM role: `getIdFromNem12Id-role-153b7a0a`.  
- Grants permissions for:  
  - SQS polling  
  - Neptune database access  
  - CloudWatch logging  

---
## 📊 Logging Strategy

| Component          | Logging Destination           | Retention | Notes |
|--------------------|-------------------------------|-----------|-------|
| Lambda (default)   | `/aws/lambda/...`             | 30 days   | AWS-managed logs |
| Application errors | `sbm-ingester-error-log`      | 30 days   | App-level error logging |
| Execution logs     | `sbm-ingester-execution-log`  | 30 days   | Tracks job runs |
| Metrics            | `sbm-ingester-metrics-log`    | 30 days   | Business metrics |
| Parse errors       | `sbm-ingester-parse-error-log`| 30 days   | File parsing issues |
| Runtime errors     | `sbm-ingester-runtime-error-log` | 30 days | Non-parse runtime issues |
| API Gateway        | Stage logs                   | 30 days   | Enable in API Gateway |
| S3                 | Access log bucket            | 90 days   | Not yet provisioned |

---
