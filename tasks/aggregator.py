import boto3
import json
import os
import time
import sys

# --- Configuration ---
# These environment variables are passed by worker.py when starting the task
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
PROJECT_NAME = os.environ.get("PROJECT_NAME")
TOTAL_AUDIO_FILES = int(os.environ.get("TOTAL_FILES", 0))
# By default, wait for both birdnet and perch models
EXPECTED_MODELS = os.environ.get("EXPECTED_MODELS", "birdnet,perch").split(",")

# Set a timeout (e.g., 4 hours) to prevent the task from hanging indefinitely
TIMEOUT_SECONDS = 4 * 60 * 60
POLL_INTERVAL = 30  # Check every 30 seconds

s3 = boto3.client("s3")


def count_s3_files(bucket, prefix):
    """
    Count JSON files under an S3 prefix using paginator
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    count = 0
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith(".json"):
                    count += 1
    return count


def get_all_results(bucket, project, models):
    """
    Download and merge all small JSON result files
    """
    combined_results = {
        "project": project,
        "summary": {},
        "files": {},  # Use filename as the key when merging
    }

    for model in models:
        prefix = f"results/{project}/{model}/"
        print(f"📥 Downloading results for {model}...")

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if not obj["Key"].endswith(".json"):
                        continue

                    # Download JSON file content
                    try:
                        response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                        data = json.loads(response["Body"].read().decode("utf-8"))

                        # Get the original filename (fall back to key name)
                        source_filename = os.path.basename(obj["Key"]).replace(
                            ".json", ""
                        )

                        if source_filename not in combined_results["files"]:
                            combined_results["files"][source_filename] = {
                                "filename": source_filename,
                                "models": {},
                            }

                        # Store result under the model name
                        combined_results["files"][source_filename]["models"][
                            model
                        ] = data

                    except Exception as e:
                        print(f"⚠️ Error reading file {obj['Key']}: {e}")

    return combined_results


def main():
    print(f"--- Aggregator Started ---")
    print(f"Project: {PROJECT_NAME}")
    print(f"Total audio files: {TOTAL_AUDIO_FILES}")
    print(f"Waiting for models: {EXPECTED_MODELS}")

    # Calculate expected total number of result JSON files (files * models)
    expected_total_jsons = TOTAL_AUDIO_FILES * len(EXPECTED_MODELS)
    print(f"Target: waiting for {expected_total_jsons} JSON result files...")

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > TIMEOUT_SECONDS:
            print("❌ Error: Aggregation timeout. Some tasks may have failed.")
            sys.exit(1)

        current_total = 0
        status_msg = []

        # Check progress for each model
        for model in EXPECTED_MODELS:
            prefix = f"results/{PROJECT_NAME}/{model}/"
            count = count_s3_files(BUCKET_NAME, prefix)
            current_total += count
            status_msg.append(f"{model}: {count}/{TOTAL_AUDIO_FILES}")

        print(
            f"⏳ Progress: {current_total}/{expected_total_jsons} | {' | '.join(status_msg)}"
        )

        # Check if processing is complete
        if current_total >= expected_total_jsons:
            print("✅ All result files are ready! Starting merge...")
            break

        time.sleep(POLL_INTERVAL)

    # --- Merging Stage ---
    try:
        final_data = get_all_results(BUCKET_NAME, PROJECT_NAME, EXPECTED_MODELS)

        # Convert to list format (for easier frontend use)
        final_report_list = list(final_data["files"].values())

        report_payload = {
            "project_name": PROJECT_NAME,
            "generated_at": datetime.now().isoformat(),
            "total_files": TOTAL_AUDIO_FILES,
            "results": final_report_list,
        }

        # Upload the final report
        report_key = f"results/{PROJECT_NAME}/final_report.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=report_key,
            Body=json.dumps(report_payload),
            ContentType="application/json",
        )

        print(f"🎉 Success! Final report generated: s3://{BUCKET_NAME}/{report_key}")

    except Exception as e:
        print(f"💥 Merge/upload failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    from datetime import (
        datetime,
    )  # Delayed import to avoid affecting earlier references

    main()
