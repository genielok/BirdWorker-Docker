import numpy as np
import pandas as pd
import tensorflow as tf
import scipy.ndimage
from datetime import datetime
from audio_utils import AudioPreprocessor


class PerchAnalyzer:
    """
    Perch Adapter.
    Uses external AudioPreprocessor for consistency.
    """

    def __init__(self, model_dir, label_path, taxonomy_path):
        print(f"Loading Perch model from {model_dir}...")

        # Load model
        self.model = tf.saved_model.load(model_dir)
        if "serving_default" in self.model.signatures:
            self.infer_fn = self.model.signatures["serving_default"]
        else:
            self.infer_fn = self.model.infer_tf

        # Load label maps
        self._load_label_maps(label_path, taxonomy_path)

        # Initialize your custom preprocessor (Targeting 32k for Perch)
        self.processor = AudioPreprocessor(target_sr=32000)
        self.window_seconds = 5.0

    def _load_label_maps(self, label_path, taxonomy_path):
        # Load label.csv (Model Output ID -> eBird Code)
        df_labels = pd.read_csv(label_path)
        if "ebird2021" in df_labels.columns:
            self.id_to_code = df_labels["ebird2021"].to_dict()
        else:
            # Assume first column is ID, second is Code
            self.id_to_code = df_labels.iloc[:, 1].to_dict()

        # Load taxonomy.csv (eBird Code -> Scientific Name)
        df_tax = pd.read_csv(taxonomy_path)
        code_col = next(
            (c for c in df_tax.columns if "SPECIES_CODE" in c.upper()), "SPECIES_CODE"
        )
        sci_col = next(
            (c for c in df_tax.columns if "SCI_NAME" in c.upper()), "SCI_NAME"
        )
        com_col = next(
            (c for c in df_tax.columns if "PRIMARY_COM_NAME" in c.upper()),
            "PRIMARY_COM_NAME",
        )

        self.code_to_meta = df_tax.set_index(code_col)[[sci_col, com_col]].to_dict(
            "index"
        )

    def _prepare_audio(self, audio_path):
        """
        Uses AudioPreprocessor to load, filter (optional), and segment audio.
        """
        # 1. Load and process using your custom class
        temp_path = self.processor.create_denoised_temp_file(audio_path)

        if not temp_path:
            return None, None

        import librosa

        # Load the processed temp file
        audio, _ = librosa.load(temp_path, sr=32000)

        # Pad if audio is too short
        window_samples = int(32000 * self.window_seconds)
        if len(audio) < window_samples:
            padding = window_samples - len(audio)
            audio = np.pad(audio, (0, padding), "constant")

        # Generate windows (Non-overlapping for Perch default)
        step = window_samples
        windows = []
        timestamps = []

        for i in range(0, len(audio) - window_samples + 1, step):
            window = audio[i : i + window_samples]
            windows.append(window)
            timestamps.append(i / 32000)

        # Clean up temp file to save space
        import os

        try:
            os.remove(temp_path)
        except:
            pass

        if len(windows) == 0:
            return None, None

        return np.stack(windows).astype(np.float32), timestamps

    def analyze(self, audio_path: str, min_conf: float = 0.4, date: datetime = None):
        # 1. Get sliced data using preprocessor logic
        wins, t_stamps = self._prepare_audio(audio_path)
        if wins is None or len(wins) == 0:
            return []

        # 2. Convert to Tensor input [batch_size, 160000]
        tf_wins = tf.convert_to_tensor(wins)

        # 3. Inference
        if "inputs" in self.infer_fn.structured_input_signature[1]:
            outputs = self.infer_fn(inputs=tf_wins)
        else:
            outputs = self.infer_fn(tf_wins)

        # 4. Get Logits and convert to probabilities
        keys = list(outputs.keys())
        logits = outputs.get("label", outputs.get("output_0", outputs[keys[0]]))
        probs = tf.math.sigmoid(logits).numpy()

        # 5. Post-processing
        if len(probs) > 1:
            probs = scipy.ndimage.uniform_filter1d(
                probs, size=3, axis=0, mode="nearest"
            )

        # 6. Extract results
        final_results = []
        for i in range(len(probs)):
            for cid in np.where(probs[i] > min_conf)[0]:
                code = self.id_to_code.get(cid)
                if not code:
                    continue

                meta = self.code_to_meta.get(code, {})
                final_results.append(
                    {
                        "start_time": t_stamps[i],
                        "end_time": t_stamps[i] + self.window_seconds,
                        "label": code,
                        "common_name": meta.get("PRIMARY_COM_NAME", code),
                        "scientific_name": meta.get("SCI_NAME", "Unknown"),
                        "confidence": float(probs[i][cid]),
                    }
                )

        final_results.sort(key=lambda x: x["confidence"], reverse=True)
        return final_results
