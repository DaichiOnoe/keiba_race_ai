import pickle
from pathlib import Path

import pandas as pd
import yaml

DATA_DIR = Path("..", "data")
MODEL_DIR = DATA_DIR / "03_train"


def predict(
    features: pd.DataFrame,
    model_dir: Path = MODEL_DIR,
    model_filename: Path = "model.pkl",
    calibration_model_filename: Path = "calibration_model.pkl",
    config_filepath: Path = "config.yaml",
):
    with open(config_filepath, "r") as f:
        feature_cols = yaml.safe_load(f)["features"]
    with open(model_dir / model_filename, "rb") as f:
        model = pickle.load(f)
    with open(model_dir / calibration_model_filename, "rb") as f:
        calibration_model = pickle.load(f)
    prediction_df = features[["race_id", "umaban", "tansyo_odds", "popularity"]].copy()
    prediction_df["pred"] = model.predict(features[feature_cols])
    prediction_df["pred_calibrated"] = calibration_model.predict(prediction_df["pred"])
    prediction_df["expect_return"] = (
        prediction_df["pred"] * prediction_df["tansyo_odds"]
    )
    prediction_df["expect_return_calibrated"] = (
        prediction_df["pred_calibrated"] * prediction_df["tansyo_odds"]
    )
    return prediction_df.sort_values("expect_return_calibrated", ascending=False)