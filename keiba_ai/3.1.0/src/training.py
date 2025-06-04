import pickle
from pathlib import Path

import lightgbm as lgb
import matplotlib.pyplot as plt
import pandas as pd
import yaml
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss

DATA_DIR = Path("..", "data")
INPUT_DIR = DATA_DIR / "02_features"
OUTPUT_DIR = DATA_DIR / "03_train"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


class Dataset:
    def __init__(
        self,
        train_start_date: str,
        train_end_date: str,
        valid_start_date: str,
        valid_end_date: str,
        test_start_date: str | None = None,
        test_end_date: str | None = None,
        input_dir: Path = INPUT_DIR,
        features_filename: str = "features.csv",
        target_strategy: int = 1,
    ) -> None:
        """
        学習用のデータセットを作成するクラス
        - 使用する期間をYYYY-MM-DD形式で指定して、学習データ、検証データ、テストデータに分割する
            - train_start_date：学習データの開始日
            - valid_start_date：検証データの開始日
            - valid_end_date：検証データの終了日
            - test_start_date：テストデータの開始日オフライン検証時のみ使用）
            - test_end_date：テストデータの終了日（オフライン検証時のみ使用）
        - target_strategy：学習時の目的変数（`target_mod`列）の作成方法を指定
        （詳しくは#22の記事: https://note.com/dijzpeb/n/nef0d54028813 を参照）
            - 0：rank==1のみを1、それ以外を0とする
            - 1：rank==1またはtime_rank==1かつ着差がハナの場合を1、それ以外を0とする
            - 2：time_rank==1のみを1、それ以外を0とする
        """
        # print(f"train_start_date: {train_start_date}")
        features = pd.read_csv(input_dir / features_filename,sep="\t", chunksize=10000) # チャンクサイズを指定して１マン行ずつ読み込む
        features = pd.concat([chunk for chunk in features], ignore_index=True)
        # print(f"features.shape: {features.shape}")
        # 目的変数
        features["target"] = (features["rank"] == 1).astype(int)
        # 補正した目的変数
        if target_strategy == 0:
            features["target_mod"] = features["target"]
        elif target_strategy == 1:
            features["target_mod"] = (
                (features["rank"] == 1)
                | (features["time_rank"] == 1) & (features["着差"] == "ハナ")
            ).astype(int)
        elif target_strategy == 2:
            features["target_mod"] = (features["time_rank"] == 1).astype(int)
        else:
            raise ValueError("target_strategyは0, 1, 2のいずれかを指定してください。")
        # 学習データとテストデータに分割
        if test_start_date is None or test_end_date is None:
            self.train_df = features.query(
                "date >= @train_start_date and date < @valid_start_date"
            )
            self.valid_df = features.query("date >= @valid_start_date")
            self.test_df = None
        else:
            self.train_df = features.query(
                "date >= @train_start_date and date < @train_end_date"
            )
            self.valid_df = features.query(
                "date >= @valid_start_date and date < @valid_end_date"
            )
            self.test_df = features.query(
                "date >= @test_start_date and date <= @test_end_date"
            )


class Trainer:
    def __init__(
        self,
        config_filepath: Path = "config.yaml",
        output_dir: Path = OUTPUT_DIR,
    ):
        with open(config_filepath, "r") as f:
            config = yaml.safe_load(f)
            self.feature_cols = config["features"]
            self.params = config["params"]
        self.output_dir = output_dir

    def run(
        self,
        dataset: Dataset,
        importance_filename: str = "importance",
        model_filename: str = "model.pkl",
        calibration_model_filename: str = "calibration_model.pkl",
        evaluation_filename: str = "evaluation.csv",
    ) -> pd.DataFrame | None:
        # データセットの作成
        lgb_train = lgb.Dataset(
            dataset.train_df[self.feature_cols], dataset.train_df["target_mod"]
        )
        lgb_valid = lgb.Dataset(
            dataset.valid_df[self.feature_cols],
            dataset.valid_df["target_mod"],
            reference=lgb_train,
        )
        # 学習の実行
        model = lgb.train(
            params=self.params,
            train_set=lgb_train,
            num_boost_round=10000,
            valid_sets=[lgb_valid],
            callbacks=[
                lgb.log_evaluation(100),
                lgb.early_stopping(stopping_rounds=100),
            ],
        )
        with open(self.output_dir / model_filename, "wb") as f:
            pickle.dump(model, f)
        # 特徴量重要度の可視化
        lgb.plot_importance(
            model, importance_type="gain", figsize=(30, 15), max_num_features=50
        )
        plt.savefig(self.output_dir / f"{importance_filename}.png")
        plt.close()
        importance_df = pd.DataFrame(
            {
                "feature": model.feature_name(),
                "importance": model.feature_importance(importance_type="gain"),
            }
        ).sort_values("importance", ascending=False)
        importance_df.to_csv(
            self.output_dir / f"{importance_filename}.csv",
            index=False,
            sep="\t",
        )
        # キャリブレーション補正
        ir = IsotonicRegression(out_of_bounds="clip")
        df_ir_train = dataset.valid_df.copy()
        df_ir_train["pred"] = model.predict(
            df_ir_train[self.feature_cols], num_iteration=model.best_iteration
        )
        ir.fit(df_ir_train["pred"], df_ir_train["target"])
        with open(self.output_dir / calibration_model_filename, "wb") as f:
            pickle.dump(ir, f)
        if dataset.test_df is not None:
            # テストデータに対してスコアリング
            evaluation_df = dataset.test_df[
                [
                    "race_id",
                    "horse_id",
                    "target",
                    "rank",
                    "tansho_odds",
                    "popularity",
                    "umaban",
                ]
            ].copy()
            evaluation_df["pred"] = model.predict(
                dataset.test_df[self.feature_cols], num_iteration=model.best_iteration
            )
            logloss = log_loss(evaluation_df["target"], evaluation_df["pred"])
            print("-" * 20 + " result " + "-" * 20)
            print(f"test_df's binary_logloss: {logloss}")
            evaluation_df["pred_calibrated"] = ir.predict(evaluation_df["pred"])
            # evaluation_df.to_csv(
            #     self.output_dir / evaluation_filename, sep="\t", index=False
            # )
            updating_rawdf(
                new_df=evaluation_df,
                key="race_id",
                save_filename=evaluation_filename,
                save_dir= self.output_dir,
            )
            return evaluation_df
    
def updating_rawdf(
    new_df: pd.DataFrame,
    key: str,
    save_filename: str,
    save_dir: Path,
) -> None:
    """
    既存のrawdfに新しいデータを追加して保存する関数。
    """
    save_dir.mkdir(parents=True, exist_ok=True)
    if (save_dir / save_filename).exists():
        old_df = pd.read_csv(save_dir / save_filename, sep="\t", dtype={f"{key}": str})
        # 念の為、key列をstr型に変換
        new_df[key] = new_df[key].astype(str)
        df = pd.concat([old_df[~old_df[key].isin(new_df[key])], new_df])
        df.to_csv(save_dir / save_filename, sep="\t", index=False)
    else:
        new_df.to_csv(save_dir / save_filename, sep="\t", index=False)
