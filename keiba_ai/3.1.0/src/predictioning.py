import pickle
from pathlib import Path
import pandas as pd
import yaml
import re
from collections import defaultdict
from playwright.async_api import async_playwright

DATA_DIR = Path("..", "data")
MODEL_DIR = DATA_DIR / "03_train"
PREDICT_DIR = DATA_DIR / "05_prediction"


def predict(
    features: pd.DataFrame,
    model_dir: Path = MODEL_DIR,
    model_filename: Path = "model.pkl",
    calibration_model_filename: Path = "calibration_model.pkl",
    config_filepath: Path = "config.yaml",
    save_dir: Path = PREDICT_DIR,
    save_filename: Path = "prediction.csv",
    sort_col: str = "expect_return_calibrated",
) -> pd.DataFrame:
    
    with open(config_filepath, "r") as f:
        feature_cols = yaml.safe_load(f)["features"]
    with open(model_dir / model_filename, "rb") as f:
        model = pickle.load(f)
    with open(model_dir / calibration_model_filename, "rb") as f:
        calibration_model = pickle.load(f)
    prediction_df = features[["race_id", "umaban", "tansho_odds", "popularity"]].copy()
    prediction_df["pred"] = model.predict(features[feature_cols])
    prediction_df["pred_calibrated"] = calibration_model.predict(prediction_df["pred"])
    prediction_df["expect_return"] = (
        prediction_df["pred"] * prediction_df["tansho_odds"]
    )
    prediction_df["expect_return_calibrated"] = (
        prediction_df["pred_calibrated"] * prediction_df["tansho_odds"]
    )
    save_dir.mkdir(exist_ok=True, parents=True) 
    # prediction_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    updating_rawdf(
        prediction_df,
        key="race_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    return prediction_df.sort_values(sort_col, ascending=False)

def updating_rawdf(
    new_df: pd.DataFrame,
    key: str,
    save_filename: str = "prediction.csv",
    save_dir: Path = PREDICT_DIR,
) -> None:
    """
    既存のpredictionファイルに新しいデータを追加して保存する関数。
    """
    save_dir.mkdir(parents=True, exist_ok=True)
    if (save_dir / save_filename).exists():
        old_df = pd.read_csv(save_dir / save_filename, sep="\t", dtype={f"{key}": str})
        # 念の為、key列をstr型に変換
        new_df[key] = new_df[key].astype(str)
        # old_dfのrace_idの中でnew_dfのrace_idに含まれないものを取出し、old_dfと定義、その後new_dfを追加
        df = pd.concat([old_df[~old_df[key].isin(new_df[key])], new_df])
        df.to_csv(save_dir / save_filename, sep="\t", index=False)
    else:
        new_df.to_csv(save_dir / save_filename, sep="\t", index=False)




async def scrape_race_time_table(kaisai_date: str, headless: bool = True) -> pd.DataFrame:
    """
    指定日のレースIDと発送時刻を取得してDataFrameで返す関数。
    
    Args:
        kaisai_date (str): 開催日（例: "20250412"）
        headless (bool): ブラウザを非表示にするか（デフォルトTrue）

    Returns:
        pd.DataFrame: race_id と post_time を含むタイムテーブル
    """
    time_table_dict = defaultdict(list)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(
            f"https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}",
            wait_until="domcontentloaded"
            # wait_until="networkidle",
            # timeout=60000,
        )
        # html_content = await page.content()
        # print(html_content)  # ページのHTMLを出力して確認
        await page.wait_for_selector("li.RaceList_DataItem", timeout=6000)  # タイムアウトを6秒に設定
        race_list = page.locator("li.RaceList_DataItem")
        for i in range(await race_list.count()):
            race = race_list.nth(i)
            href = await race.locator("a").first.get_attribute("href")
            race_id = re.findall(r"race_id=(\d{12})", href)[0]
            time_table_dict["race_id"].append(race_id)
            # レース名を取得
            race_name = await race.locator("span.ItemTitle").inner_text()
            time_table_dict["race_name"].append(race_name.strip())
            #発送時刻を取得
            post_time = await race.locator("span.RaceList_Itemtime").inner_text()
            time_table_dict["post_time"].append(post_time.strip())
       
        await browser.close()
        # await context.close()
    

    df = pd.DataFrame(time_table_dict)
    df = df.sort_values("post_time", ascending=True).reset_index(drop=True)
    
    return df

