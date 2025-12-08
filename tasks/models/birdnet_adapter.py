import uuid
from datetime import datetime
from birdnetlib import Recording
from birdnetlib.analyzer import Analyzer

THRESHOLD = 0.5


class BirdNetAnalyzer:
    def __init__(self, min_conf=THRESHOLD):
        self.min_conf = min_conf
        self.analyzer = Analyzer()
        print(f"Loaded BirdNET (version {self.analyzer.version})")

    def analyze(self, audio_path: str, date: datetime):
        """
        audio_path: audio file path
        date: datetime
        key: S3‘s key (optional)
        """
        recording = Recording(
            analyzer=self.analyzer, path=audio_path, date=date, min_conf=self.min_conf
        )

        recording.analyze()

        detections = []
        for det in recording.detections:
            det["id"] = str(uuid.uuid4())
            det["model_version"] = self.analyzer.version
            detections.append(det)

        return detections
