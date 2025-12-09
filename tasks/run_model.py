import os
import sys
import json
import boto3
import warnings
import traceback

from datetime import datetime
from models import load_model  # Unified model loader

print("--- RUNNING VERSION: unified-model-runner (with Skip Logic) ---")

# -----------------------------------------------------------------
# 1. Configuration
# -----------------------------------------------------------------
INPUT_BUCKET = os.environ.get("S3_BUCKET_NAME")
OUTPUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX", "results/birdnet")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "unknown")
MODEL_NAME = os.environ.get("MODEL_NAME", "birdnet")

INPUT_KEYS_JSON = os.environ.get("S3_INPUT_KEYS")
if not INPUT_BUCKET or not INPUT_KEYS_JSON:
    print("FATAL: S3_BUCKET_NAME and S3_INPUT_KEYS must be set.")
    sys.exit(1)

try:
    INPUT_KEYS = [obj["key"] for obj in json.loads(INPUT_KEYS_JSON)]
except Exception as e:
    print(f"FATAL: Bad S3_INPUT_KEYS JSON: {e}")
    sys.exit(1)

TEMP_DIR = "/tmp/audio_work"
os.makedirs(TEMP_DIR, exist_ok=True)

s3 = boto3.client("s3")

# Load model globally
try:
    model = load_model(MODEL_NAME)
    print(f"MODEL LOADED: {MODEL_NAME}")
except Exception as e:
    print(f"FATAL: Failed to load model: {e}")
    sys.exit(1)


# -----------------------------------------------------------------
# 2. Process a single file
# -----------------------------------------------------------------
def process_single_file(key: str):
    local_filename = os.path.basename(key)
    local_audio_path = os.path.join(TEMP_DIR, local_filename)

    # 构造结果文件的 S3 路径
    result_key = f"{OUTPUT_PREFIX}/{local_filename}.json"
    local_result_path = os.path.join(TEMP_DIR, f"{local_filename}.json")

    # ‼️ 1. Idempotency Check) ‼️
    try:
        s3.head_object(Bucket=INPUT_BUCKET, Key=result_key)
        print(f"⏩ [Skip] Result already exists: {result_key}")
        return result_key
    except Exception:
        pass

    try:
        print(f"⬇️ Downloading s3://{INPUT_BUCKET}/{key}")
        s3.download_file(INPUT_BUCKET, key, local_audio_path)

        if os.path.getsize(local_audio_path) < 1024:
            raise ValueError("File too small or corrupted (<1KB).")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            detections = model.analyze(local_audio_path, datetime.now())

            for det in detections:
                det["source_s3_key"] = key
                det["source_filename"] = local_filename

        result_json = {
            "source_bucket": INPUT_BUCKET,
            "source_key": key,
            "analysis_model": MODEL_NAME,
            "status": "success",
            "detections": detections,
            "processed_at": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"❌ Processing failed for {key}: {e}")
        result_json = {
            "source_bucket": INPUT_BUCKET,
            "source_key": key,
            "analysis_model": MODEL_NAME,
            "status": "error",
            "error_message": str(e),
            "detections": [],
        }

    try:
        with open(local_result_path, "w") as f:
            json.dump(result_json, f, indent=2)

        s3.upload_file(local_result_path, INPUT_BUCKET, result_key)
        print(f"⬆️ Uploaded result (status={result_json.get('status')}) → {result_key}")
        return result_key

    except Exception as upload_error:
        print(f"💥 Fatal: Upload failed for {key}: {upload_error}")
        return None

    finally:
        # 清理临时文件
        if os.path.exists(local_audio_path):
            os.remove(local_audio_path)
        if os.path.exists(local_result_path):
            os.remove(local_result_path)


# -----------------------------------------------------------------
# 3. Entrypoint
# -----------------------------------------------------------------
if __name__ == "__main__":
    print(f"--- Batch Start ({len(INPUT_KEYS)} files) ---")

    all_results = []
    for key in INPUT_KEYS:
        r = process_single_file(key)
        if r:
            all_results.append(r)

    summary = {
        "project": PROJECT_NAME,
        "processed_files": len(all_results),
        "result_keys": all_results,
    }
    print(json.dumps(summary))
