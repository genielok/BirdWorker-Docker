import uuid
from datetime import datetime
from birdnetlib import Recording
from birdnetlib.analyzer import Analyzer

THRESHOLD = 0.5
OVERLAP = 0.5


class BirdNetAnalyzer:
    def __init__(self, min_conf=THRESHOLD):
        self.min_conf = min_conf
        self.analyzer = Analyzer()
        print(f"Loaded BirdNET (version {self.analyzer.version})")

    def analyze(self, audio_path: str, date: datetime, lat=None, lon=None):
        recording = Recording(
            analyzer=self.analyzer,
            path=audio_path,
            date=None,
            lat=lat,
            lon=lon,
            min_conf=self.min_conf,
            overlap=OVERLAP,
        )

        try:
            recording.analyze()
        except Exception as e:
            print(f"BirdNET analysis failed: {e}")
            return []

        detections = []
        for det in recording.detections:
            normalized_det = {
                "id": str(uuid.uuid4()),
                "model": "BirdNET",
                "model_version": self.analyzer.version,
                "confidence": det.get("confidence"),
                "label": det.get(
                    "label"
                ),  # 注意：BirdNET返回的是 "Common Name_Scientific Name" 这种格式，可能需要拆分
                "scientific_name": det.get("scientific_name"),
                "common_name": det.get("common_name"),
                "start_time": det.get("start_time"),
                "end_time": det.get("end_time"),
                "date": date.isoformat(),
            }
            detections.append(normalized_det)

        detections.sort(key=lambda x: x["confidence"], reverse=True)

        return detections
