import boto3
import json
import os
import time
import math
import urllib.parse

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

CONTAINER_NAME_BIRDNET = os.environ.get("CONTAINER_NAME_BIRDNET", "birdnet-worker")
CONTAINER_NAME_PERCH = os.environ.get("CONTAINER_NAME_PERCH", "perch-worker")
CONTAINER_NAME_AGGREGATOR = os.environ.get(
    "CONTAINER_NAME_AGGREGATOR", "birdnet-worker"
)

# Initialize AWS clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
ecs = boto3.client("ecs", region_name=AWS_REGION)


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

    try:
        ecs.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=task_def,
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
    print(f"👀 Launching Aggregator Task (TaskDef: {TASK_DEF_AGGREGATOR})...")

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
                        "name": CONTAINER_NAME_AGGREGATOR,
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
        print("✅ Aggregator launched!")
    except Exception as e:
        print(f"💥 Failed to launch Aggregator: {e}")


def process_manifest(manifest_key):
    print(f"📄 Processing manifest: {manifest_key}")

    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        manifest = json.loads(obj["Body"].read())

        project_name = manifest.get("project_name", "unknown_project")
        all_files = manifest.get("audio_files", [])
        total_files = len(all_files)

        if not all_files:
            print("⚠️ Empty manifest, skipping.")
            return

        print(f"📊 Project: {project_name} | Total Files: {total_files}")

        BATCH_SIZE = 50
        total_batches = math.ceil(total_files / BATCH_SIZE)

        for i in range(total_batches):
            start = i * BATCH_SIZE
            end = start + BATCH_SIZE
            batch_files = all_files[start:end]

            launch_analysis_task("birdnet", project_name, batch_files, i + 1)
            launch_analysis_task("perch", project_name, batch_files, i + 1)

        launch_aggregator_task(project_name, total_files)

    except Exception as e:
        print(f"❌ Processing failed: {e}")


def poll_queue():
    print(f"Worker listening on: {SQS_QUEUE_URL}")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=20
            )
            if "Messages" in response:
                for msg in response["Messages"]:
                    receipt_handle = msg["ReceiptHandle"]
                    try:
                        body_str = msg["Body"]
                        body = json.loads(body_str)

                        if "Records" in body:
                            for record in body["Records"]:
                                if "s3" in record:
                                    key = urllib.parse.unquote_plus(
                                        record["s3"]["object"]["key"]
                                    )
                                    if key.endswith("manifest.json"):
                                        process_manifest(key)

                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )

                    except json.JSONDecodeError:
                        print(f"❌ Received non-JSON message: {msg['Body']}")
                        print(
                            "🗑️ Invalid message detected, deleting to avoid infinite retry loop..."
                        )
                        sqs.delete_message(
                            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                        )

                    except Exception as inner_e:
                        print(f"⚠️ Error processing message: {inner_e}")
                        # Do NOT delete message → allow SQS retry

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
