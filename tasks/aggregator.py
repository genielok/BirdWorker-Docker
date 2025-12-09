import boto3
import json
import os
import time
import sys
from datetime import datetime

# --- Configuration ---
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
PROJECT_NAME = os.environ.get("PROJECT_NAME")
TOTAL_AUDIO_FILES = int(os.environ.get("TOTAL_FILES", 0))
EXPECTED_MODELS = os.environ.get("EXPECTED_MODELS", "birdnet,perch").split(",")

# Timeout Settings
TIMEOUT_SECONDS = 3 * 60 * 60  # Force stop after 3 hours
# TODO: now is 5 minutes, change back to 15 minutes later
NO_PROGRESS_TIMEOUT = (
    5 * 60
)  # If file count hasn't changed for 15 minutes, assume stuck and force settlement

s3 = boto3.client("s3")


def count_s3_files(bucket, prefix):
    """
    Count JSON files in S3 folder using paginator
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
    Download and merge all small JSON results
    """
    combined_results = {"project": project, "summary": {}, "files": {}}

    # Stats for success and failure
    stats = {m: {"success": 0, "error": 0, "missing": 0} for m in models}

    for model in models:
        prefix = f"results/{project}/{model}/"
        print(f"📥 Downloading results for {model}...", flush=True)

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        found_files = set()

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if not obj["Key"].endswith(".json"):
                        continue

                    try:
                        # Download and parse
                        response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                        data = json.loads(response["Body"].read().decode("utf-8"))

                        s3_filename_key = os.path.basename(obj["Key"])[:-5]

                        if s3_filename_key not in combined_results["files"]:
                            combined_results["files"][s3_filename_key] = {
                                "filename": s3_filename_key,
                                "models": {},
                            }

                        combined_results["files"][s3_filename_key]["models"][
                            model
                        ] = data
                        found_files.add(s3_filename_key)

                        if data.get("status") == "error":
                            stats[model]["error"] += 1
                        else:
                            stats[model]["success"] += 1

                    except Exception as e:
                        print(f"⚠️ Error reading file {obj['Key']}: {e}", flush=True)

        stats[model]["missing"] = TOTAL_AUDIO_FILES - len(found_files)

    combined_results["summary"] = stats
    return combined_results


def main():
    print(f"--- Aggregator Started ---", flush=True)
    print(
        f"Project: {PROJECT_NAME} | Target File Count: {TOTAL_AUDIO_FILES}", flush=True
    )

    expected_total_jsons = TOTAL_AUDIO_FILES * len(EXPECTED_MODELS)
    start_time = time.time()

    last_count = 0
    last_change_time = time.time()

    while True:
        now = time.time()

        if now - start_time > TIMEOUT_SECONDS:
            print("⚠️ Warning: Aggregation timed out, forcing settlement...", flush=True)
            break

        current_total = 0
        status_msg = []

        for model in EXPECTED_MODELS:
            prefix = f"results/{PROJECT_NAME}/{model}/"
            count = count_s3_files(BUCKET_NAME, prefix)
            current_total += count
            status_msg.append(f"{model}: {count}/{TOTAL_AUDIO_FILES}")

        # 2. Check if all completed
        if current_total >= expected_total_jsons:
            print("✅ All result files ready!", flush=True)
            break

        # 3. Check for "stall" (progress bar not moving for a long time)
        if current_total > last_count:
            last_count = current_total
            last_change_time = now
        elif now - last_change_time > NO_PROGRESS_TIMEOUT:
            print(
                f"⚠️ Warning: No new results for {int(NO_PROGRESS_TIMEOUT/60)} minutes. Assuming some tasks crashed. Forcing settlement...",
                flush=True,
            )
            break

        # ‼️ Critical Fix: flush=True ensures logs show up in CloudWatch in real-time
        print(
            f"⏳ Progress: {current_total}/{expected_total_jsons} | {' | '.join(status_msg)}",
            flush=True,
        )
        time.sleep(30)

    # --- Merge Phase ---
    print("📦 Starting to package final report...", flush=True)
    try:
        final_data = get_all_results(BUCKET_NAME, PROJECT_NAME, EXPECTED_MODELS)

        report_payload = {
            "project_name": PROJECT_NAME,
            "generated_at": datetime.now().isoformat(),
            "total_files_expected": TOTAL_AUDIO_FILES,
            "summary": final_data["summary"],
            "results": list(final_data["files"].values()),
        }

        # Upload report
        report_key = f"results/{PROJECT_NAME}/final_report.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=report_key,
            Body=json.dumps(report_payload),
            ContentType="application/json",
        )

        print(
            f"🎉 Final report generated (with missing stats): s3://{BUCKET_NAME}/{report_key}",
            flush=True,
        )
        print(
            f"Stats summary: {json.dumps(final_data['summary'], indent=2)}", flush=True
        )

    except Exception as e:
        print(f"💥 Merge upload failed: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
