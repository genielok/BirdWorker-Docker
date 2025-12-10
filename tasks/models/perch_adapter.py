import numpy as np
import pandas as pd
import soundfile as sf
import tensorflow as tf
from datetime import datetime
import librosa
import scipy.signal

THRESHOLD = 0.7
SAMPLE_RATE = 32000


class PerchAnalyzer:
    def __init__(self, model_dir, label_path, taxonomy_path):
        """
        initialize Perch model analyzer
        """
        print(f"Loading model from {model_dir}...")
        self.model = tf.saved_model.load(model_dir)
        self.infer_fn = self.model.infer_tf
        self.target_sr = SAMPLE_RATE
        self.window_seconds = 5.0
        self.window_samples = int(self.window_seconds * self.target_sr)

        print("Loading taxonomy maps...")
        self._load_label_maps(label_path, taxonomy_path)

    def _load_label_maps(self, label_path, taxonomy_path):
        df_labels = pd.read_csv(label_path)
        self.id_to_code = df_labels["ebird2021"].to_dict()

        df_tax = pd.read_csv(taxonomy_path)
        self.code_to_meta = df_tax.set_index("SPECIES_CODE")[
            ["SCI_NAME", "PRIMARY_COM_NAME"]
        ].to_dict("index")

    def _load_and_resample(self, audio_path: str) -> np.ndarray:
        try:
            audio, _ = librosa.load(audio_path, sr=self.target_sr, mono=True)
        except Exception as e:
            print(f"Error loading audio with librosa: {e}")
            return np.array([], dtype="float32")
        return audio.astype("float32")

    def _high_pass_filter(self, audio: np.ndarray, cutoff=200) -> np.ndarray:
        if len(audio) == 0:
            return audio
        sos = scipy.signal.butter(10, cutoff, "hp", fs=self.target_sr, output="sos")
        filtered = scipy.signal.sosfilt(sos, audio)
        return filtered.astype("float32")

    def _make_windows(self, waveform: np.ndarray, overlap=0.5) -> (np.ndarray, list):
        total_samples = waveform.shape[0]
        step = int(self.window_samples * (1 - overlap))

        windows = []
        timestamps = []

        if total_samples < self.window_samples:
            padded = np.zeros(self.window_samples, dtype="float32")
            padded[:total_samples] = waveform
            return np.array([padded]), [0.0]

        for start in range(0, total_samples - self.window_samples + 1, step):
            end = start + self.window_samples
            windows.append(waveform[start:end])
            timestamps.append(start / self.target_sr)

        return np.array(windows, dtype="float32"), timestamps

    def _suppress_duplicates(self, detections, time_gap_threshold=3.0):
        if not detections:
            return []

        detections.sort(key=lambda x: x["confidence"], reverse=True)
        final_detections = []

        while detections:
            best = detections.pop(0)
            final_detections.append(best)

            # filter out duplicates, if audio events are too close in time, only keep the best one
            detections = [
                d
                for d in detections
                if not (
                    d["label"] == best["label"]
                    and abs(d["start_time"] - best["start_time"]) < time_gap_threshold
                )
            ]

        return sorted(final_detections, key=lambda x: x["start_time"])

    def analyze(self, audio_path: str, date: datetime, threshold: float = THRESHOLD):
        """
        Enhanced analysis pipeline
        """
        # 1. Load & Resample
        waveform = self._load_and_resample(audio_path)
        if len(waveform) == 0:
            return []

        # 2. Noise Suppression (High Pass)
        waveform = self._high_pass_filter(waveform)

        # 3. Create Overlapping Windows
        # overlap=0.5 means each 2.5s will appear in two 5s
        windows, timestamps = self._make_windows(waveform, overlap=0.5)

        # Batch inference
        tf_windows = tf.convert_to_tensor(windows, dtype=tf.float32)
        outputs = self.infer_fn(tf_windows)

        label_logits = outputs["label"]  # Keep as tensor for sigmoid

        probabilities = tf.math.sigmoid(label_logits).numpy()

        # reduce noise by smoothing over time
        probabilities = scipy.ndimage.uniform_filter1d(
            probabilities, size=3, axis=0, mode="nearest"
        )

        raw_detections = []
        num_windows = probabilities.shape[0]

        for i in range(num_windows):
            high_prob_indices = np.where(probabilities[i] > threshold)[0]

            start_time = timestamps[i]

            for class_id in high_prob_indices:
                confidence = float(probabilities[i][class_id])
                code = self.id_to_code.get(class_id, "Unknown_Code")
                meta = self.code_to_meta.get(code, {})

                detection = {
                    "confidence": confidence,
                    "label": code,
                    "scientific_name": meta.get("SCI_NAME", "Unknown"),
                    "common_name": meta.get("PRIMARY_COM_NAME", "Unknown"),
                    "start_time": start_time,
                    "end_time": start_time + self.window_seconds,
                    "date": date.isoformat(),  # 记录日期
                }
                raw_detections.append(detection)

        # (NMS) remove duplicates
        clean_results = self._suppress_duplicates(raw_detections)

        return clean_results
