Bird Analysis Worker (Orchestrator)

This is a Python-based background orchestration service that listens to an AWS SQS queue and triggers large-scale audio analysis tasks on AWS Fargate based on uploaded manifest files (manifest.json).

It serves as the core "Commander" of the entire Bird Species Analysis Pipeline.

🏗 Architecture

[S3 Upload] -> [S3 Event Notification] -> [SQS Queue] -> [Worker Service] -> [AWS ECS Fargate API]

Listen: The Worker performs a continuous Long Polling loop on a specific SQS queue.

Trigger: When manifest.json is uploaded to S3, S3 sends an event to SQS, and the Worker receives the message.

Parse: The Worker downloads and parses the manifest file to get the list of audio files.

Batching: It splits thousands of files into small batches (default 50 files per batch) to avoid hitting environment variable limits.

Dispatch: It calls the AWS ECS run_task API to launch multiple Fargate tasks (BirdNET and Perch) in parallel.

Aggregate: Once all analysis tasks are dispatched, it launches a final Aggregator task to wait for and summarize the results.

📋 Prerequisites

Before running this service, ensure the following infrastructure is ready on AWS:

AWS SQS: A standard queue (e.g., BirdAnalysisQueue) configured with S3 event notifications.

AWS ECS Cluster: A Fargate cluster (e.g., bird-analysis-cluster).

ECS Task Definitions:

birdnet-task: Task definition for running the BirdNET model.

perch-task: Task definition for running the Google Perch model.

aggregator-task: (Optional) Task definition for the aggregator script, usually reuses birdnet-task.

Network: A Public Subnet within a VPC and a Security Group allowing outbound traffic.

IAM Role: The Worker requires ecs:RunTask, iam:PassRole, sqs:ReceiveMessage, and s3:GetObject permissions.

⚙️ Environment Variables

This service is configured entirely via environment variables. Set the following variables when running the Docker container or deploying to ECS:

Core Configuration

Variable

Description

Example

AWS_REGION

AWS Region

eu-north-1

SQS_QUEUE_URL

Full SQS Queue URL

https://sqs.eu-north-1.amazonaws.com/123.../MyQueue

S3_BUCKET_NAME

S3 Bucket name for audio and results

my-birdnet-bucket

ECS Network Configuration (Worker must know where to launch tasks)

Variable

Description

Example

ECS_CLUSTER

Target ECS Cluster Name

bird-analysis-cluster

SUBNET_ID

Subnet ID for tasks (Public IP enabled)

subnet-0a1b2c...

SECURITY_GROUP_ID

Security Group ID for tasks

sg-012345...

Task Definitions

Variable

Description

Default

TASK_DEF_BIRDNET

BirdNET Task Definition Name:Version

birdnet-task:1

TASK_DEF_PERCH

Perch Task Definition Name:Version

perch-task:1

TASK_DEF_AGGREGATOR

Aggregator Task Definition Name:Version

birdnet-task:1

CONTAINER_NAME_AGGREGATOR

Container name inside Aggregator Task

birdnet-worker

🚀 Local Development & Testing

You can run this Worker locally in Docker to test the scheduling logic (it will connect to real AWS services).

1. Build Image

docker build -t bird-worker:local .


2. Run Container

Create a .env file with the variables above, then run:

docker run --env-file .env \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  bird-worker:local


(Note: Pass AWS credentials when running locally; use IAM Task Role when deploying to ECS)

🚢 Deployment to AWS (Production)

1. Push to ECR

# Login to AWS ECR
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com

# Build (amd64 architecture for Fargate)
docker build --platform linux/amd64 -t bird-worker:latest .

# Tag
docker tag bird-worker:latest <ACCOUNT_ID>.dkr.ecr.<REGION>[.amazonaws.com/bird-worker:v1](https://.amazonaws.com/bird-worker:v1)

# Push
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>[.amazonaws.com/bird-worker:v1](https://.amazonaws.com/bird-worker:v1)


2. Update ECS Service

Go to the AWS ECS Console.

Find the Task Definition for worker.

Create a New Revision, updating the Image URI to the one pushed above (.../bird-worker:v1).

Update the ECS Service (bird-worker-service), select the new revision, and check Force new deployment.

📜 License

[Your License Here]
