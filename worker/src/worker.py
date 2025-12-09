import boto3
import json
import os
import time
import math
import urllib.parse
import random
from botocore.exceptions import ClientError

# --- 1. Basic Configuration ---
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# --- 2. AWS Network Configuration (Required for Fargate) ---
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "bird-analysis-cluster")
SUBNET_ID = os.environ.get("SUBNET_ID")
SECURITY_GROUP_ID = os.environ.get("SECURITY_GROUP_ID")

# --- 3. Task Definitions ---
TASK_DEF_BIRDNET = os.environ.get("TASK_DEF_BIRDNET", "birdnet-task:1")
TASK_DEF_PERCH = os.environ.get("TASK_DEF_PERCH", "perch-task:1")
TASK_DEF_AGGREGATOR = os.environ.get("TASK_DEF_AGGREGATOR", TASK_DEF_BIRDNET)

# --- 4. Container Names (Must match Container Name in ECS Task Definition) ---
CONTAINER_NAME_BIRDNET = os.environ.get("CONTAINER_NAME_BIRDNET", "birdnet-worker")
CONTAINER_NAME_PERCH = os.environ.get("CONTAINER_NAME_PERCH", "perch-worker")
CONTAINER_NAME_AGGREGATOR = os.environ.get(
    "CONTAINER_NAME_AGGREGATOR", "birdnet-worker"
)

# Initialize Clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
ecs = boto3.client("ecs", region_name=AWS_REGION)


# ==========================================
# Logic 2: Auto-Retry (Decorator)
# ==========================================
def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """
    Decorator: Automatically retry on AWS throttling errors with exponential backoff.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    if e.response["Error"]["Code"] in [
                        "ThrottlingException",
                        "Throttling",
                        "RequestLimitExceeded",
                    ]:
                        if x == retries:
                            raise
                        # Exponential backoff + jitter
                        sleep_time = backoff_in_seconds * 2**x + random.uniform(0, 1)
                        print(f"⚠️ AWS API Throttled, retrying in {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
                        x += 1
                    else:
                        raise

        return wrapper

    return decorator


# ==========================================
# Logic 1: Deduplication Check (S3 Based)
# ==========================================
def is_job_completed_in_s3(project_name):
    """
    Check if the final report already exists in S3 to prevent duplicate processing.
    Assumes the aggregator writes to: results/{project_name}/final_report.json
    """
    report_key = f"results/{project_name}/final_report.json"

    try:
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=report_key)
        # If head_object succeeds, the file exists
        print(
            f"🔁 Duplicate detected: '{report_key}' already exists in S3. Skipping job."
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # File not found, safe to proceed
            return False
        else:
            # Other errors (e.g., permissions), log and proceed just in case
            print(f"⚠️ S3 check failed: {e}. Proceeding with job.")
            return False


# --- ECS Task Launch Logic ---


@retry_with_backoff(retries=5)
def launch_fargate_task_api(task_def, container_name, env_vars):
    """Low-level API call to run_task"""
    return ecs.run_task(
        cluster=ECS_CLUSTER,
        taskDefinition=task_def,
        # launchType="FARGATE",  <-- REMOVED: Cannot use both launchType and capacityProviderStrategy
        # Use Spot to save costs
        capacityProviderStrategy=[
            {"capacityProvider": "FARGATE_SPOT", "weight": 1, "base": 0}
        ],
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": [SUBNET_ID],
                "securityGroups": [SECURITY_GROUP_ID],
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [{"name": container_name, "environment": env_vars}]
        },
    )


def launch_analysis_task(model_type, project_name, file_batch, batch_index):
    if model_type == "perch":
        task_def = TASK_DEF_PERCH
        output_prefix = f"results/{project_name}/perch"
        container_name = CONTAINER_NAME_PERCH
    else:
        task_def = TASK_DEF_BIRDNET
        output_prefix = f"results/{project_name}/birdnet"
        container_name = CONTAINER_NAME_BIRDNET

    print(
        f"🚀 [Batch {batch_index}] Launching {model_type} task ({len(file_batch)} files)..."
    )

    input_keys_json = json.dumps([{"key": k} for k in file_batch])

    env_vars = [
        {"name": "S3_BUCKET_NAME", "value": S3_BUCKET_NAME},
        {"name": "PROJECT_NAME", "value": project_name},
        {"name": "MODEL_NAME", "value": model_type},
        {"name": "S3_OUTPUT_PREFIX", "value": output_prefix},
        {"name": "S3_INPUT_KEYS", "value": input_keys_json},
    ]

    try:
        launch_fargate_task_api(task_def, container_name, env_vars)
    except Exception as e:
        print(f"💥 Failed to launch analysis task: {e}")
        raise e


def launch_aggregator_task(project_name, total_files):
    print(f"👀 Launching Aggregator Task (TaskDef: {TASK_DEF_AGGREGATOR})...")

    env_vars = [
        {"name": "S3_BUCKET_NAME", "value": S3_BUCKET_NAME},
        {"name": "PROJECT_NAME", "value": project_name},
        {"name": "TOTAL_FILES", "value": str(total_files)},
        {"name": "EXPECTED_MODELS", "value": "birdnet,perch"},
    ]

    try:
        ecs.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=TASK_DEF_AGGREGATOR,
            capacityProviderStrategy=[
                {"capacityProvider": "FARGATE_SPOT", "weight": 1, "base": 0}
            ],
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [SUBNET_ID],
                    "securityGroups": [SECURITY_GROUP_ID],
                    "assignPublicIp": "ENABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": CONTAINER_NAME_AGGREGATOR,
                        "command": ["python", "-u", "aggregator.py"],
                        "environment": env_vars,
                    }
                ]
            },
        )
        print("✅ Aggregator launched!")
    except Exception as e:
        print(f"💥 Failed to launch Aggregator: {e}")
        raise e


def process_manifest(manifest_key):
    print(f"📄 Processing manifest: {manifest_key}")

    try:
        # 1. Download and parse manifest first to get project_name
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        manifest = json.loads(obj["Body"].read())

        project_name = manifest.get("project_name", "unknown_project")
        all_files = manifest.get("audio_files", [])
        total_files = len(all_files)

        if not all_files:
            print("⚠️ Empty manifest, skipping.")
            return

        # 2. 🛡️ Deduplication Check (S3 Based)
        # Check if the result file already exists
        if is_job_completed_in_s3(project_name):
            print(f"✅ Job for project '{project_name}' is already done. Skipping.")
            return

        print(f"📊 Project: {project_name} | Total Files: {total_files}")

        BATCH_SIZE = 50
        total_batches = math.ceil(total_files / BATCH_SIZE)

        for i in range(total_batches):
            start = i * BATCH_SIZE
            end = start + BATCH_SIZE
            batch_files = all_files[start:end]

            # Parallel launch
            launch_analysis_task("birdnet", project_name, batch_files, i + 1)
            launch_analysis_task("perch", project_name, batch_files, i + 1)

        launch_aggregator_task(project_name, total_files)

    except Exception as e:
        print(f"❌ Process failed: {e}")
        # Raise exception to trigger SQS retry logic
        raise e


# ==========================================
# Logic 3: SQS Polling & DLQ (Error Handling)
# ==========================================
def poll_queue():
    print(f"Worker listening on: {SQS_QUEUE_URL}")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                AttributeNames=["ApproximateReceiveCount"],
            )

            if "Messages" in response:
                for msg in response["Messages"]:
                    receipt_handle = msg["ReceiptHandle"]
                    # Log retry count for debugging
                    receive_count = msg.get("Attributes", {}).get(
                        "ApproximateReceiveCount", "1"
                    )

                    try:
                        body = json.loads(msg["Body"])
                        if "Records" in body:
                            for record in body["Records"]:
                                if "s3" in record:
                                    key = urllib.parse.unquote_plus(
                                        record["s3"]["object"]["key"]
                                    )
                                    if key.endswith("manifest.json"):
                                        print(
                                            f"Received msg (Attempt #{receive_count}): {key}"
                                        )
                                        process_manifest(key)

                        # ✅ Delete message only on success
                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )

                    except json.JSONDecodeError:
                        print(f"❌ Invalid JSON, deleting: {msg['Body'][:20]}...")
                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )

                    except Exception as inner_e:
                        print(f"⚠️ Task failed (Message retained for retry): {inner_e}")
                        # 🛡️ Crucial: Do NOT delete message.
                        # Let VisibilityTimeout expire so SQS retries it.
                        # After maxReceiveCount, AWS moves it to DLQ.

        except Exception as e:
            print(f"Polling connection error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    missing_vars = []
    if not SQS_QUEUE_URL:
        missing_vars.append("SQS_QUEUE_URL")
    if not SUBNET_ID:
        missing_vars.append("SUBNET_ID")
    if not S3_BUCKET_NAME:
        missing_vars.append("S3_BUCKET_NAME")

    if missing_vars:
        print(f"❌ Fatal: Missing environment variables: {', '.join(missing_vars)}")
        exit(1)
    else:
        poll_queue()
