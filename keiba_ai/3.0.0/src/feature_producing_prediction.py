import json
import random
import re
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from tqdm.notebook import tqdm
from webdriver_manager.chrome import ChromeDriverManager

# commonディレクトリのパス
COMMON_DATA_DIR = Path("..", "..", "common", "data")
POPULATION_DIR = COMMON_DATA_DIR / "prediction_population"
MAPPING_DIR = COMMON_DATA_DIR / "mapping"
# v3_0_0ディレクトリのパス
DATA_DIR = Path("..", "data")
INPUT_DIR = DATA_DIR / "01_preprocessed"
OUTPUT_DIR = DATA_DIR / "02_features"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# カテゴリ変数を数値に変換するためのマッピング
with open(MAPPING_DIR / "sex.json", "r") as f:
    sex_mapping = json.load(f)
with open(MAPPING_DIR / "race_type.json", "r") as f:
    race_type_mapping = json.load(f)
with open(MAPPING_DIR / "around.json", "r") as f:
    around_mapping = json.load(f)
with open(MAPPING_DIR / "weather.json", "r") as f:
    weather_mapping = json.load(f)
with open(MAPPING_DIR / "ground_condition.json", "r") as f:
    ground_condition_mapping = json.load(f)
with open(MAPPING_DIR / "race_class.json", "r") as f:
    race_class_mapping = json.load(f)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    ]
    
class PredictionFeatureMaker:
    def __init__(
        self,
        population_dir: Path = POPULATION_DIR,
        population_filename: str = "predict_population.csv",
        input_dir: Path = INPUT_DIR,
        horse_results_filename: Path = "horse_results_prediction.csv",
        output_dir: Path = OUTPUT_DIR,
        output_filename: str = "features_prediction.csv",
    ):
        
        self.population = pd.read_csv(population_dir / population_filename, sep="\t")
        self.horse_results = pd.read_csv(input_dir / horse_results_filename, sep="\t")
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.htmls = {}
        
    def create_baselog(self):
        """
        horse_resultsをレース結果テーブルの日付よりも過去に絞り、集計元のログを作成。
        """
        self.baselog = (
            self.population.merge(
                self.horse_results, on="horse_id", suffixes=("", "_horse")
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
        )
    

    def agg_horse_n_races(self, n_races: list[int] = [3, 5, 10, 1000]) -> None:
        """
        直近nレースの着順と賞金の平均を集計する関数。
        出走馬が確定時点で先に実行しておいても良い。
        """
        grouped_df = self.baselog.groupby(["race_id", "horse_id"])
        merged_df = self.population.copy()
        for n_race in n_races:
            df = (
                grouped_df.head(n_race)
                .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                .mean()
                .add_suffix(f"_{n_race}races")
            )
            merged_df = merged_df.merge(df, on=["race_id", "horse_id"])
        self.agg_horse_n_races_df = merged_df
        
    
    def fetch_shutuba_page_html(self, race_id: str) -> None:
        """
       レースidを設定すると、出馬表ページのhtmlをスクレイピングする関数
        """
        
        print("fetching shubuta page html...")
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--user-agent=" + random.choice(USER_AGENTS))
        # chrome driverをインストール
        # driver_path = ChromeDriverManager().install()
        driver_path = 'C:/Users/Onoe Daichi/Downloads/競馬AI/keiba_ai/common/src/chromedriver-win32/chromedriver.exe'
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        with webdriver.Chrome(service=Service(driver_path), options=options) as driver:
            driver.implicitly_wait(10)
            driver.get(url)
            self.htmls[race_id] = driver.page_source

    def fetch_results(
        self, race_id: str, html: str, sex_mapping: dict = sex_mapping
    ) -> None:
        """
        出馬表ページのhtmlを受け取ると、
        「レース結果テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        df = pd.read_html(html)[0]
        df.columns = df.columns.get_level_values(1)
        soup = BeautifulSoup(html, "lxml").find("table", class_="Shutuba_Table")
        horse_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/horse/"))
        for a in a_list:
            horse_id = re.findall(r"\d{10}", a["href"])[0]
            horse_id_list.append(int(horse_id))
        df["horse_id"] = horse_id_list
        jockey_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/jockey/"))
        for a in a_list:
            jockey_id = re.findall(r"\d{5}", a["href"])[0]
            jockey_id_list.append(int(jockey_id))
        df["jockey_id"] = jockey_id_list
        trainer_id_list = []
        a_list = soup.find_all("a", href=re.compile(r"/trainer/"))
        for a in a_list:
            trainer_id = re.findall(r"\d{5}", a["href"])[0]
            trainer_id_list.append(int(trainer_id))
        df["trainer_id"] = trainer_id_list
        # 前処理
        df = df[df.iloc[:, 2] != "取消"]
        df["wakuban"] = df.iloc[:, 0].astype(int)
        df["umaban"] = df.iloc[:, 1].astype(int)
        df["sex"] = df.iloc[:, 4].str[0].map(sex_mapping)
        df["age"] = df.iloc[:, 4].str[1:].astype(int)
        df["impost"] = df.iloc[:, 5].astype(float)
        df["weight"] = df.iloc[:, 8].str.extract(r"(\d+)").astype(int)
        df["weight_diff"] = (
            df.iloc[:, 8]
            .str.replace("前計不", "0", regex=False)
            .str.extract(r"\((.+)\)")
            .fillna(0)
            .astype(int)
        )
        df["tansho_odds"] = df.iloc[:, 9].astype(float)
        df["popularity"] = df.iloc[:, 10].astype(int)
        df["race_id"] = int(race_id)
        df["n_horses"] = df.groupby("race_id")["race_id"].transform("count")
        # 使用する列を選択
        df = df[
            [
                "race_id",
                "horse_id",
                "jockey_id",
                "trainer_id",
                "umaban",
                "wakuban",
                "tansho_odds",
                "popularity",
                "impost",
                "sex",
                "age",
                "weight",
                "weight_diff",
                "n_horses",
            ]
        ]
        self.results = df
    
    def fetch_race_info(
        self,
        race_id: str,
        html: str,
        race_type_mapping: dict = race_type_mapping,
        around_mapping: dict = around_mapping,
        weather_mapping: dict = weather_mapping,
        ground_condition_mapping: dict = ground_condition_mapping,
        race_class_mapping: dict = race_class_mapping,
                        
        ) -> pd.DataFrame:
        """
        出馬表のhtmlを受け取り、レース情報テーブルを取得して、学習時と同じ形式で前処理する関数。
        """
        info_dict = {}
        info_dict["race_id"] = int(race_id)
        soup = BeautifulSoup(html, "lxml").find("div", class_="RaceList_Item02")
        title = soup.find("h1").text.strip()
        divs = soup.find_all("div")
        div1 = divs[0].text.replace(" ", "")
        info1 = re.findall(r"[\w:]+", div1)
        info_dict["race_type"] = race_type_mapping[info1[1][0]]
        info_dict["around"] = (
            around_mapping[info1[2][0]] if info_dict["race_type"] != 2 else np.nan
        )
        info_dict["course_len"] = int(re.findall(r"\d+", info1[1])[0])
        info_dict["weather"] = weather_mapping[re.findall(r"天候:(\w+)", div1)[0]]
        info_dict["ground_condition"] = ground_condition_mapping[
            re.findall(r"馬場:(\w+)", div1)[0]
        ]
        # レース階級情報の取得
        regex_race_class = "|".join(race_class_mapping)
        race_class_title = re.findall(regex_race_class, title)
        # タイトルからレース階級情報が取れない場合
        race_class = re.findall(regex_race_class, divs[1].text)
        if len(race_class_title) != 0:
            info_dict["race_class"] = race_class_mapping[race_class_title[0]]
        elif len(race_class) != 0:
            info_dict["race_class"] = race_class_mapping[race_class[0]]
        else:
            info_dict["race_class"] = None
        info_dict["place"] = int(race_id[4:6])
        self.race_info = pd.DataFrame(info_dict, index=[0])
    
    def make_features(
        self, race_id:str, skip_agg_horse: bool = False) -> pd.DataFrame:
        """
        特徴量作成処理を実行し、populationテーブルに全ての特徴量を結合する。
        """
        # 馬の過去成績テーブル集計
        # 先に実行しておいた場合はskip_agg_horse=Trueとすればスキップできる
        if not skip_agg_horse:
            self.create_baselog()    
            self.agg_horse_n_races()
        # 各種テーブルの取得
        self.fetch_shutuba_page_html(race_id)
        self.fetch_results(race_id, self.htmls[race_id])
        self.fetch_race_info(race_id, self.htmls[race_id])
        # 全ての特徴量を結合
        features = (
            self.population.merge(self.results, on=["race_id", "horse_id"])
            .merge(self.race_info, on=["race_id"])
            .merge(
                self.agg_horse_n_races_df,
                on=["race_id", "date", "horse_id"],
                how="left",
            )
         )
        features.to_csv(self.output_dir / "features.csv", sep="\t", index=None)
        return features
        