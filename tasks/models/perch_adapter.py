import numpy as np
import pandas as pd
import soundfile as sf
import tensorflow as tf
from datetime import datetime

THRESHOLD = 0.7


class PerchAnalyzer:
    def __init__(self, model_dir, label_path, taxonomy_path):
        """
        initialize Perch model analyzer
        :param model_dir: SavedModel folder path
        :param label_path: label.csv file path (for id -> eBird code)
        :param taxonomy_path: eBird_taxonomy_v2025.csv file path (for eBird code -> names)
        """
        print(f"Loading model from {model_dir}...")
        self.model = tf.saved_model.load(model_dir)
        self.infer_fn = self.model.infer_tf
        self.target_sr = 32000
        self.window_seconds = 5.0
        self.window_samples = int(self.window_seconds * self.target_sr)

        print("Loading taxonomy maps...")
        self._load_label_maps(label_path, taxonomy_path)

    def _load_label_maps(self, label_path, taxonomy_path):
        df_labels = pd.read_csv(label_path)
        self.id_to_code = df_labels["ebird2021"].to_dict()

        # 2.load taxonomy
        df_tax = pd.read_csv(taxonomy_path)
        # build code_to_meta  key=SPECIES_CODE, value={SCI_NAME, PRIMARY_COM_NAME}
        self.code_to_meta = df_tax.set_index("SPECIES_CODE")[
            ["SCI_NAME", "PRIMARY_COM_NAME"]
        ].to_dict("index")

    def _load_and_resample(self, audio_path: str) -> np.ndarray:
        audio, sr = sf.read(audio_path, dtype="float32")
        if audio.ndim > 1:
            audio = np.mean(audio, axis=1)
        if sr != self.target_sr:
            duration = audio.shape[0] / sr
            target_len = int(duration * self.target_sr)
            x_old = np.linspace(0, 1, num=audio.shape[0], endpoint=False)
            x_new = np.linspace(0, 1, num=target_len, endpoint=False)
            audio = np.interp(x_new, x_old, audio).astype("float32")
        return audio

    def _make_windows(self, waveform: np.ndarray) -> np.ndarray:
        total_samples = waveform.shape[0]
        if total_samples <= self.window_samples:
            padded = np.zeros(self.window_samples, dtype="float32")
            padded[:total_samples] = waveform
            return padded[None, :]
        num_windows = int(np.ceil(total_samples / self.window_samples))
        windows = np.zeros((num_windows, self.window_samples), dtype="float32")
        for i in range(num_windows):
            start = i * self.window_samples
            end = min(start + self.window_samples, total_samples)
            chunk = waveform[start:end]
            windows[i, : chunk.shape[0]] = chunk
        return windows

    def analyze(self, audio_path: str, date: datetime, threshold: float = THRESHOLD):
        """
        Analyze an audio file and return detections above the confidence threshold.
        :param audio_path: audio file path
        :param threshold:  (0.0 - 1.0)
        """
        waveform = self._load_and_resample(audio_path)
        windows = self._make_windows(waveform)
        tf_windows = tf.convert_to_tensor(windows, dtype=tf.float32)

        outputs = self.infer_fn(tf_windows)

        # get Logits and convert to probabilities
        label_logits = outputs["label"].numpy()
        probabilities = tf.nn.softmax(label_logits, axis=-1).numpy()

        results = []

        # iterate over each window
        num_windows = probabilities.shape[0]
        for window_idx in range(num_windows):
            # find classes with prob > threshold
            high_prob_indices = np.where(probabilities[window_idx] > threshold)[0]

            for class_id in high_prob_indices:
                confidence = float(probabilities[window_idx][class_id])

                code = self.id_to_code.get(class_id, "Unknown_Code")
                meta = self.code_to_meta.get(code, {})

                detection = {
                    "confidence": confidence,
                    "label": code,  # ebird code
                    "scientific_name": meta.get("SCI_NAME", "Unknown"),
                    "common_name": meta.get("PRIMARY_COM_NAME", "Unknown"),
                    "start_time": window_idx * self.window_seconds,
                    "end_time": (window_idx + 1) * self.window_seconds,
                }
                results.append(detection)

        # sort by confidence descending
        results.sort(key=lambda x: x["confidence"], reverse=True)

        return results
