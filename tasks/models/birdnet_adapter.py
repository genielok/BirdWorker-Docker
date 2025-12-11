import os
from datetime import datetime
from typing import List, Dict, Any
from birdnetlib import Recording
from birdnetlib.analyzer import Analyzer
from audio_utils import AudioPreprocessor


class BirdNetAnalyzer:
    def __init__(self):
        self.analyzer = Analyzer()
        self.preprocessor = AudioPreprocessor(target_sr=48000)

    def analyze(
        self,
        audio_path: str,
        min_conf: float = 0.1,
        lat: float = None,
        lon: float = None,
        date: datetime = None,
        cleanup: bool = True,
    ) -> List[Dict[str, Any]]:

        temp_path = None
        try:
            temp_path = self.preprocessor.create_denoised_temp_file(audio_path)
            if not temp_path:
                print("❌ Failed to create temp file.")
                return []

            recording = Recording(
                analyzer=self.analyzer,
                path=audio_path,
                lat=lat,
                lon=lon,
                date=date or datetime.now(),
                min_conf=min_conf,
            )

            recording.analyze()

            detections = []
            for det in recording.detections:
                if det["confidence"] < min_conf:
                    continue

                detections.append(
                    {
                        "common_name": det["common_name"],
                        "scientific_name": det["scientific_name"],
                        "confidence": det["confidence"],
                        "start_time": det["start_time"],
                        "end_time": det["end_time"],
                        "label": det["label"],
                    }
                )

            detections.sort(key=lambda x: x["confidence"], reverse=True)
            return detections

        except Exception as e:
            print(f"❌ Analysis error: {e}")
            return []

        finally:
            if cleanup and temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                    print(f"🧹 Temporary file removed: {temp_path}")
                except OSError:
                    pass
