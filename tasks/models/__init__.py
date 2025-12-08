MODEL_PATH = "perch_model"
LABEL_CSV = "perch_model/assets/label.csv"
TAXONOMY_CSV = "perch_model/assets/eBird_taxonomy_v2025.csv"


def load_model(name: str):
    if name == "birdnet":
        from .birdnet_adapter import BirdNetAnalyzer

        return BirdNetAnalyzer()
    elif name == "perch":
        from .perch_adapter import PerchAnalyzer

        return PerchAnalyzer(
            model_dir=MODEL_PATH, label_path=LABEL_CSV, taxonomy_path=TAXONOMY_CSV
        )
    else:
        raise ValueError(f"Unknown model: {name}")
