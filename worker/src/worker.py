import boto3
import json
import os
import time
import math
import urllib.parse

# --- 1. configuration ---
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
S3_BUCKET_NAME = os.environ.get("AWS_S3_BUCKET_NAME")

# --- 2. AWS network configuration (Required for Fargate) ---
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "ReactDockerCluster")
SUBNET_ID = os.environ.get("SUBNET_ID")
SECURITY_GROUP_ID = os.environ.get("SECURITY_GROUP_ID")

# --- 3. Task Definitions ---
# Corresponding Task Definitions created in AWS ECS
TASK_DEF_BIRDNET = os.environ.get("TASK_DEF_BIRDNET", "birdnet-task:1")
TASK_DEF_PERCH = os.environ.get("TASK_DEF_PERCH", "perch-task:1")
# Aggregator Task Definition (usually reuse BirdNET or Perch container image since they include boto3)
TASK_DEF_AGGREGATOR = os.environ.get("TASK_DEF_AGGREGATOR", TASK_DEF_BIRDNET)

# Initialize clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
ecs = boto3.client("ecs", region_name=AWS_REGION)


def launch_analysis_task(model_type, project_name, file_batch, batch_index):
    """
    Launch a single analysis task (BirdNET or Perch)
    """
    if model_type == "perch":
        task_def = TASK_DEF_PERCH
        output_prefix = f"results/{project_name}/perch"
        container_name = "perch-worker"  # Must match Container Name in Task Definition
    elif model_type == "birdnet":
        task_def = TASK_DEF_BIRDNET
        output_prefix = f"results/{project_name}/birdnet"
        container_name = "birdnet-worker"
    else:
        task_def = TASK_DEF_BIRDNET
        output_prefix = f"results/{project_name}/birdnet"
        container_name = "birdnet-worker"

    print(
        f"🚀 [Batch {batch_index}] Launching {model_type} task ({len(file_batch)} files)..."
    )

    # Convert file list into JSON string
    input_keys_json = json.dumps([{"key": k} for k in file_batch])

    try:
        ecs.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=task_def,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [SUBNET_ID],
                    "securityGroups": [SECURITY_GROUP_ID],
                    "assignPublicIp": "ENABLED",  # Must be enabled so Fargate can pull the container image
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "environment": [
                            {"name": "S3_BUCKET_NAME", "value": S3_BUCKET_NAME},
                            {"name": "PROJECT_NAME", "value": project_name},
                            {"name": "MODEL_NAME", "value": model_type},
                            {"name": "S3_OUTPUT_PREFIX", "value": output_prefix},
                            {"name": "S3_INPUT_KEYS", "value": input_keys_json},
                        ],
                    }
                ]
            },
        )
    except Exception as e:
        print(f"💥 Failed to launch analysis task: {e}")


def launch_aggregator_task(project_name, total_files):
    """
    Launch the aggregator task (run aggregator.py)
    """
    print(f"👀 Launching aggregator task (monitoring {total_files} result files)...")

    try:
        ecs.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=TASK_DEF_AGGREGATOR,
            launchType="FARGATE",
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
                        # Assuming BirdNET image is reused, container name = birdnet-worker
                        "name": "birdnet-worker",
                        # Override default command to run aggregator instead
                        "command": ["python", "aggregator.py"],
                        "environment": [
                            {"name": "S3_BUCKET_NAME", "value": S3_BUCKET_NAME},
                            {"name": "PROJECT_NAME", "value": project_name},
                            {"name": "TOTAL_FILES", "value": str(total_files)},
                            {"name": "EXPECTED_MODELS", "value": "birdnet,perch"},
                        ],
                    }
                ]
            },
        )
        print("✅ Aggregator started!")
    except Exception as e:
        print(f"💥 Failed to start aggregator: {e}")


def process_manifest(manifest_key):
    print(f"📄 Processing manifest file: {manifest_key}")

    try:
        # 1. Download and parse manifest
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        manifest = json.loads(obj["Body"].read())

        project_name = manifest.get("project_name", "unknown_project")
        all_files = manifest.get("audio_files", [])
        total_files = len(all_files)

        if not all_files:
            print("⚠️ Empty manifest, skipping.")
            return

        print(f"📊 Project: {project_name} | Total files: {total_files}")

        # 2. Split file list into batches (to avoid overly long env variables)
        BATCH_SIZE = 50
        total_batches = math.ceil(total_files / BATCH_SIZE)

        # 3. Launch analysis tasks for all batches
        for i in range(total_batches):
            start = i * BATCH_SIZE
            end = start + BATCH_SIZE
            batch_files = all_files[start:end]

            # Launch both models in parallel
            launch_analysis_task("birdnet", project_name, batch_files, i + 1)
            launch_analysis_task("perch", project_name, batch_files, i + 1)

        # 4. Finally, launch aggregator
        launch_aggregator_task(project_name, total_files)

    except Exception as e:
        print(f"❌ Manifest processing failed: {e}")


def poll_queue():
    print(f"Worker listening on queue: {SQS_QUEUE_URL}")
    while True:
        try:
            # Long polling SQS
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=20
            )

            if "Messages" in response:
                for msg in response["Messages"]:
                    body = json.loads(msg["Body"])

                    # S3 event notifications may contain multiple records
                    if "Records" in body:
                        for record in body["Records"]:
                            # Decode URL-encoded S3 key
                            key = urllib.parse.unquote_plus(
                                record["s3"]["object"]["key"]
                            )

                            # Only handle manifest.json files
                            if key.endswith("manifest.json"):
                                process_manifest(key)

                    # Delete message to avoid reprocessing
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"]
                    )
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    # Simple startup check
    if not SQS_QUEUE_URL:
        print("❌ Error: SQS_QUEUE_URL environment variable not set")
    else:
        poll_queue()
