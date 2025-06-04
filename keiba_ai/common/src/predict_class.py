import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm.notebook import tqdm
import traceback
from pathlib import Path
import scraping
import create_prediction_population
import create_table


POPULATION_DIR = Path("..", "data", "prediction_population")
DATA_DIR = Path("..", "data")
HTML_DIR =  Path("..","data","html")
HTML_RACE_DIR = HTML_DIR /"race_test"
HTML_HORSE_DIR = HTML_DIR /"horse"
HTML_PED_DIR = HTML_DIR /"ped"
RAWDF_DIR = Path("..","data","rawdf")

# 当日の予測出走馬に対して、過去成績テーブル、血統テーブルのスクレイピング、作成を行う関数をまとめたクラス。 

class PredictionPopulation:
    def __init__(
        self,  
        html_path_list=None, # html_path_listはscrape_html_horse()、table_horse_results()でも使用するためインスタンス変数として定義
        upgrade_html_path_list=None, # upgrade_html_path_listはscrape_html_ped()、create_peds()でも使用するためインスタンス変数として定義
        
        # output_horse_html_dir: Path = HTML_HORSE_DIR, 
        # output_horse_dir: Path = RAWDF_DIR,
        # output_horse_filename: str = "horse_results_prediction.csv", 
        # input_peds_dir: Path = RAWDF_DIR,
        # input_peds_filename: str = "peds_prediction.csv"
        ):
        # html_path_listがNoneの場合には、新しいリストが毎回作成され、インスタンス間でリストが共有されることを防ぐ。
        if html_path_list is None:
            html_path_list = []  # Noneの場合、新しいリストを作成
        self.html_path_list = html_path_list
        if upgrade_html_path_list is None:
            upgrade_html_path_list = []  # Noneの場合、新しいリストを作成
        self.upgrade_html_path_list = upgrade_html_path_list
        
        # self.output_horse_html_dir = output_horse_html_dir
        # self.output_horse_dir = output_horse_dir
        # self.output_horse_filename = output_horse_filename
        # self.input_peds_dir = input_peds_dir
        # self.input_peds_filename = input_peds_filename
        
        
    def create(
        self,
        kaisai_date: str,
        output_population_dir: Path = POPULATION_DIR,
        output_population_filename: str = "population.csv",
    ) -> pd.DataFrame:
        """
        開催日（yyyymmdd形式）を指定すると、予測母集団である 
        (date, race_id, horse_id)のDataFrameが返ってくる関数。
        """
        print("scraping race_id_list...")
        race_id_list = scraping.scrape_race_id_list([kaisai_date])
        dfs = {}
        print("scraping horse_id_list...")
        for race_id in tqdm(race_id_list):
            horse_id_list = create_prediction_population.scrape_horse_id_list(race_id)
            time.sleep(1)
            df = pd.DataFrame(
                {"date": kaisai_date, "race_id": race_id, "horse_id": horse_id_list}
            )
            dfs[race_id] = df 
        POPULATION_DIR.mkdir(exist_ok=True, parents=True)
        concat_df = pd.concat(dfs.values())
        concat_df["date"] = pd.to_datetime(concat_df["date"], format="%Y%m%d") 
        concat_df.to_csv(output_population_dir / output_population_filename, index=False, sep="\t")
        return concat_df


    def scrape_html_horse(
        self,
        output_horse_html_dir: Path = HTML_HORSE_DIR, 
        skip: bool =False, 
        path: bool = True
    ):
        """
        horseページのhtmlをスクレイピング。save_dirに.bin形式で保存。すでにファイルが存在し、skip=Trueの場合は処理がスキップされ新規のものがプラスされて帰ってくる。
        その際、path=Trueにするとhtml_path_listにpathは追加されるようになる。
        """
        
        horse_id_list = pd.read_csv(POPULATION_DIR/ "population.csv", sep="\t")["horse_id"].unique()
        output_horse_html_dir.mkdir(parents=True, exist_ok=True)
        for horse_id in tqdm(horse_id_list):
            filepath = output_horse_html_dir / f"{horse_id}.bin"
            # binファイルが存在し、skip=Trueの場合はスキップする
            if filepath.is_file() and skip:
                # print(f"skipped:{horse_id}")
                if path:
                    self.html_path_list.append(filepath)
            else:
                url = f'https://db.netkeiba.com/horse/{horse_id}/'
                headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
            }
                req = Request(url, headers=headers)
                response = urlopen(req)
                html = response.read()
                time.sleep(0.5)
                with open(filepath, "wb") as f:
                    f.write(html)
                self.html_path_list.append(filepath)
        return self.html_path_list

    def table_horse_results(
        self,
        html_path_list: list[Path] = [],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "horse_results_prediction.csv"
    ) -> pd.DataFrame:
        """
        horseページのhtmlを読み込んでレース結果テーブルを加工する関数。リスト型でつなげる。
        """
        
        dfs = {} 
        for html_path in tqdm(html_path_list):
            with open(html_path, "rb") as f:
                
                try:
                    horse_id = html_path.stem  # stemで拡張子なしのファイル名を取得
                    html = f.read()
                    df = pd.read_html(html)[2]
                    # 最初の列にrace_idを挿入
                    df.insert(0, "horse_id", horse_id)
                    dfs[horse_id] = df
                except IndexError as e:
                    print(f"table not found at {horse_id}")
                    continue
                except ValueError as e:
                    print(f"{e} at {horse_id}")
                    continue
        concat_df = pd.concat(dfs.values())
        concat_df.columns = concat_df.columns.str.replace(" ","")
        save_dir.mkdir(parents=True, exist_ok=True)
        concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
        return concat_df

    def scrape_html_ped(
        self,
        input_ped_html_dir: Path = HTML_PED_DIR, 
        skip: bool = True, 
        path: bool = True,
    ):
        """
        血統ページののhtmlをスクレイピングしてsave_dirに保存する関数。
        skip=Trueにすると、すでにhtmlが存在する場合はスキップされる。
        逆に上書きしたい場合は、skip=Falseにする。
        その際、path=Trueにするとhtml_path_listにpathは追加されるようになる。
        """
        
        horse_id_list = pd.read_csv(POPULATION_DIR/ "population.csv", sep="\t")["horse_id"].unique()
        input_ped_html_dir.mkdir(parents=True, exist_ok=True)
        for horse_id in tqdm(horse_id_list):
            filepath = input_ped_html_dir / f"{horse_id}.bin"
            # binファイルが存在し、skip=Trueの場合はスキップする
            if filepath.is_file() and skip:
                print(f"skipped:{horse_id}")
                if path:
                    self.upgrade_html_path_list.append(filepath)
            else:
                url = f'https://db.netkeiba.com/horse/ped/{horse_id}/'
                headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
            }
                req = Request(url, headers=headers)
                html = urlopen(req).read()
                time.sleep(0.5)
                with open(filepath, "wb") as f:
                    f.write(html)
                self.upgrade_html_path_list.append(filepath)
        return self.upgrade_html_path_list

    def create_peds(
        self,
        html_path_list: list[Path],
        input_peds_dir: Path = RAWDF_DIR,
        input_peds_filename: str = "peds_prediction.csv"
    ) -> pd.DataFrame:
        """
        保存済みの馬の血統ページのhtmlを読み込んで血統テーブルを加工する関数。リスト型をつなげる。
        """
        dfs = {}
        for html_path in tqdm(html_path_list):
            with open(html_path, "rb") as f:
                try:
                    # ファイル名からhorse_idを取得
                    horse_id = html_path.stem
                    html = f.read()
                    soup = BeautifulSoup(html, "lxml").find("table", class_="blood_table detail")
                    td_list = soup.find_all("td")
                    ped_id_list = []
                    for td in td_list:
                        ped_id = re.findall(r"horse/(\w+)/", td.find("a")["href"])[0]
                        ped_id_list.append(ped_id)
                    df = pd.DataFrame(ped_id_list).T.add_prefix("ped_")
                    # 最初の列にhorse_idを挿入
                    df.insert(0, "horse_id", horse_id)
                    dfs[horse_id] = df
                except IndexError as e:
                    print(f"table not found at {horse_id}")
                    continue
                except AttributeError as e:
                    print(f"{e} at {horse_id}")
                    continue
        concat_df = pd.concat(dfs.values())
        input_peds_dir.mkdir(parents=True, exist_ok=True)
        concat_df.to_csv(input_peds_dir / input_peds_filename, sep="\t", index=False)
        return concat_df

    # 以上の関数をまとめて、実行する関数を作成する。
    def run(
        self, 
        kaisai_date: str
        ):
        """
        予測母集団の作成、出走馬の過去成績をスクレイピング,テーブルの作成、出走馬の血統をスクレイピング,テーブルの作成を行う関数。
        """
        
        # 予測母集団の作成
        self.create(kaisai_date)
        # 出走馬の過去成績をスクレイピング,テーブルの作成
        print("scraping horse_results...")
        self.scrape_html_horse(skip=False)
        self.table_horse_results(html_path_list=self.html_path_list)
        # 出走馬の血統をスクレイピング,テーブルの作成
        print("scraping peds...")
        self.scrape_html_ped()
        self.create_peds(html_path_list=self.upgrade_html_path_list)
        return "complete"