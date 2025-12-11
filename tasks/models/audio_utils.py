import os
import librosa
import numpy as np
import soundfile as sf
import scipy.signal as signal


class AudioPreprocessor:
    def __init__(self, target_sr=48000):
        self.target_sr = target_sr

    def apply_high_pass_filter(self, audio, sr, cutoff=300):
        """
        high-pass filter to remove low-frequency noise
        """
        # design a Butterworth high-pass filter
        sos = signal.butter(10, cutoff, "hp", fs=sr, output="sos")
        filtered = signal.sosfilt(sos, audio)
        return filtered

    def create_denoised_temp_file(self, input_path: str) -> str:
        try:
            # 1. read audio
            audio, sr = librosa.load(input_path, sr=self.target_sr)

            # 2. reduce 300Hz below noise, keep bird call details
            clean_audio = self.apply_high_pass_filter(audio, sr, cutoff=300)

            # 3. output temp file path
            base_name = os.path.basename(input_path)
            name_no_ext = os.path.splitext(base_name)[0]

            temp_filename = f"{name_no_ext}_filtered.wav"
            temp_path = os.path.abspath(temp_filename)

            # 4. save
            sf.write(temp_path, clean_audio, sr)

            return temp_path

        except Exception as e:
            print(f"❌ Audio processing failed for {input_path}: {e}")
            return None
