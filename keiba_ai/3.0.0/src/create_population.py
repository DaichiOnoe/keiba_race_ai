from pathlib import Path
import pandas as pd

COMMON_DIR = Path("..","..", "common","data")
RAWDF_DIR = COMMON_DIR / "rawdf"
OUTPUT_DIR = Path("..","data","00_population")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

def create(
    from_: str,
    to_: str,
    input_dir: Path = RAWDF_DIR,
    race_info_filename:str = "race_info.csv",
    results_filename:str = "results.csv",
    output_dir: Path = OUTPUT_DIR,
    output_filename: str = "population.csv",
) -> pd.DataFrame:
    """
    from_からto_までの期間を指定して(yyyy-mm-dd形式)、その期間に対応する
    学習母集団(race_id, date, horse_id)の組み合わせを作成する関数
    """
    race_info = pd.read_csv(input_dir /race_info_filename, sep="\t")
    race_info["date"] = pd.to_datetime(
       race_info["info_2"].map(lambda x: eval(x)[0]), format="%Y年%m月%d日"
    )
    # race_info["date"] = pd.to_datetime(race_info["info_2"].map(lambda x: eval(x, {"nan": np.nan})[0]), format="%Y年%m月%d日")
    # race_info["date"] = pd.to_datetime(race_info["info_2"]
    #                                    .map(lambda x: eval(x, {"nan": np.nan})[0] if isinstance(eval(x, {"nan": np.nan}), list) else np.nan), format="%Y年%m月%d日"
    # )
    results = pd.read_csv(input_dir / results_filename, sep="\t")
    population = (race_info.query("@from_ <= date <= @to_")[["race_id", "date"]]
        .merge(results[["race_id", "horse_id"]], on="race_id")
    )
    population.astype({"race_id": "int64"})
    population.to_csv(output_dir / output_filename, sep="\t", index=False)
    return population
    

