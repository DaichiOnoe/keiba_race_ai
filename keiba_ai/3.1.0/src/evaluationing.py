import math
from collections import defaultdict
from pathlib import Path
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

DATA_DIR = Path("..","data")
PREPROCESSED_DIR = DATA_DIR/"01_preprocessed"
TRAIN_DIR = DATA_DIR / "03_train"
OUTPUT_DIR = DATA_DIR/"04_evaluation"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def plot_calibration(
    df: pd.DataFrame,
    pred_col: str = "pred",
    target_col: str = "target",
    n_bins: int = 20,
    strategy: str = "quantile",
):
    brier_score = brier_score_loss(df[target_col], df[pred_col])
    prob_true, prob_pred = calibration_curve(
        df[target_col], df[pred_col], n_bins=n_bins, strategy=strategy
    )
    plt.figure(figsize=(5, 4))
    plt.plot(
        prob_pred,
        prob_true,
        marker="o",
        label=f"Model (Brier score: {brier_score:.5f})",
    )
    plt.plot(
        [0.001, 1],
        [0.001, 1],
        linestyle="--",
        color="black",
        label="Perfectly calibrated",
    )
    plt.xlabel("Predicted probability")
    plt.ylabel("True probability")
    plt.title("Calibration plot")
    if strategy == "quantile":
        plt.xscale("log")
        plt.yscale("log")
    plt.legend()
    plt.show()

class Evaluator:
    def __init__(
        self,
        return_tables_filepath: Path = PREPROCESSED_DIR/ "return_tables.pickle",
        train_dir: Path = TRAIN_DIR,
        evaluation_filename: str ="evaluation.csv",
        output_dir :Path = OUTPUT_DIR
     ):

        self.return_tables =  pd.read_pickle(return_tables_filepath)
        self.evaluation_df = pd.read_csv(train_dir / evaluation_filename,sep="\t")
        self.output_dir = output_dir
        
        
    def box_top_n(
        self,
        sort_col: str = "pred",
        ascending: bool = False,
        n: int = 3,
        exp_name: str = "model",
    ):
        """
        sort_colで指定した列でソートし、上位n件のBOX馬券の的中率・回収率を
        シミュレーションする関数。
        """
        bet_df = (
            self.evaluation_df.sort_values(sort_col, ascending=ascending)
            .groupby("race_id")
            .head(n)
            .groupby("race_id")["umaban"]
            # 払い戻しテーブルの馬番は文字列型なので合わせる
            .apply(lambda x: list(x.astype(str)))
            .reset_index()
        )
        df = bet_df.merge(self.return_tables, on="race_id")
        # 的中判定。BOXで全通り賭けるシミュレーションなので、集合の包含関係で判定できる。
        df["hit"] = df.apply(
            lambda x: set(x["umaban_y"]).issubset(set(x["umaban_x"])), axis=1
        )
        # 馬券種ごとの的中率
        agg_hitrate = (
            df.groupby(["race_id", "bet_type"])["hit"]
            .max()
            .groupby("bet_type")
            .mean()
            .rename(f"hitrate_{exp_name}")
            .to_frame()
        )
        # 馬券種ごとの回収率
        df["hit_return"] = df["return"] * df["hit"]
        n_bets_dict = {
            "単勝": n,
            "複勝": n,
            "馬連": math.comb(n, 2),
            "ワイド": math.comb(n, 2),
            "馬単": math.perm(n, 2),
            "三連複": math.comb(n, 3),
            "三連単": math.perm(n, 3),
        }
        agg_df = df.groupby(["race_id", "bet_type"])["hit_return"].sum().reset_index()
        agg_df["n_bets"] = agg_df["bet_type"].map(n_bets_dict)
        agg_df = (
            agg_df.query("n_bets > 0")
            .groupby("bet_type")[["hit_return", "n_bets"]]
            .sum()
        )
        agg_returnrate = (
            (agg_df["hit_return"] / agg_df["n_bets"] / 100)
            .rename(f"returnrate_{exp_name}")
            .to_frame()
            .reset_index()
        )
        output_df =pd.merge(agg_hitrate, agg_returnrate, on="bet_type")
        output_df.insert(0, "topn", n)
        return output_df
    
    
    def summarize_box_top_n(
        self,
        sort_col: str = "pred",
        ascending: bool = False,
        n: int = 3,
        exp_name: str = "model",
        save_filename: str = "box_summary.csv",
    ) -> pd.DataFrame:
        """
        topnの的中率・回収率を人気順モデルと比較してまとめる関数。
        """
        summary_df = pd.merge(
            self.box_top_n("popularity", True, n, "pop"),  # 人気順モデル
            self.box_top_n(sort_col, ascending, n, exp_name),  # 実験モデル
            on=["topn", "bet_type"],
        )
        summary_df.to_csv(self.output_dir / save_filename, sep="\t")
        return summary_df

    
    def summarize_box_exp(
        self, sort_col: str = "pred", exp_name: str = "model"
    ) -> pd.DataFrame:
        """
        top1~3の的中率・回収率をまとめる関数。実験モデルの比較に使用。
        """
        summary_df = pd.concat(
            [
                self.box_top_n(n=1, exp_name=exp_name, sort_col=sort_col),
                self.box_top_n(n=2, exp_name=exp_name, sort_col=sort_col),
                self.box_top_n(n=3, exp_name=exp_name, sort_col=sort_col),
            ]
        )
        return summary_df
    
    def bet_by_expect_return(
            self,
            min_exp: int,
            max_exp: int,
            n_samples: int = 100,
            pred_col: str = "pred",
            odds_col: str = "tansho_odds",
            target_col: str = "target",
            min_pred: float = 0.0,
            save_filename: str = "by_expect_return.png",
            table_save_filename: str = "expect_return.csv",
        ) -> pd.DataFrame:
            """
            以下の馬券購入戦略において、回収率とシャープレシオをプロットする関数。
            ・期待値がある値{exp}を超えたものに単勝馬券を賭ける
            ・ただし、予測確率が{min_pred}未満のものは賭けない
            """
            df = self.evaluation_df.copy()
            df["expect_return"] = df[odds_col] * df[pred_col]
            df["payoff"] = df[target_col] * df[odds_col]
            result = defaultdict(list)
            for exp in np.linspace(min_exp, max_exp, n_samples):
                bet_df = df.query(f"expect_return > {exp} & {pred_col} >= {min_pred}")
                total_bet = len(bet_df)
                total_payoff = bet_df["payoff"].sum()
                return_rate = total_payoff / total_bet
                payoff_per_race = bet_df.groupby("race_id")["payoff"].sum()
                n_hits = bet_df["target"].sum()
                result["expect_return"].append(exp)
                result["total_bet"].append(total_bet)
                result["total_payoff"].append(total_payoff)
                result["return_rate"].append(return_rate)
                result["std"].append(   
                    np.sqrt(len(payoff_per_race)) * payoff_per_race.std() / len(bet_df) 
                )
                result["n_hits"].append(n_hits)
            result_df = pd.DataFrame(result)
            result_df["sharp_ratio"] = (result_df["return_rate"] - 1) / result_df["std"]
            # 結果をプロット
            fig, ax1 = plt.subplots()
            sns.lineplot(
                result_df, x="expect_return", y="return_rate", ax=ax1, label="return rate"
            )
            ax1.fill_between(
                result_df["expect_return"],
                y1=result_df["return_rate"] - result_df["std"],
                y2=result_df["return_rate"] + result_df["std"],
                alpha=0.3,
            )
            ax1.legend(loc="upper left")
            ax2 = ax1.twinx()
            sns.lineplot(
                result_df,
                x="expect_return",
                y="sharp_ratio",
                ax=ax2,
                color="red",
                label="sharp ratio",
            )
            ax2.grid(False)
            ax2.legend(loc="upper right")
            fig.savefig(self.output_dir / save_filename)
            print("best result:")
            pprint(result_df.loc[result_df["sharp_ratio"].idxmax()].to_dict())
            result_df.to_csv(self.output_dir / table_save_filename, sep="\t")
        #     updating_rawdf(
        #     result_df,
        #     key="race_id",
        #     save_filename=table_save_filename,
        #     save_dir=OUTPUT_DIR,
        # )
            return result_df

    def plot_calibration(
        self, pred_col="pred", target_col="target", n_bins=20, strategy="quantile"
        ):
        plot_calibration(self.evaluation_df, pred_col, target_col, n_bins, strategy)


def updating_rawdf(
    self,
    new_df: pd.DataFrame,
    key: str,
    save_filename: str,
    save_dir: Path ,
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
