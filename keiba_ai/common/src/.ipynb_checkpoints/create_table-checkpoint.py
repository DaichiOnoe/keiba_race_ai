import pandas as pd
from pathlib import Path
from tqdm.notebook import tqdm
import traceback
from bs4 import BeautifulSoup
import re


RAWDF_DIR = Path("..","data","rawdf")

def table_results(
    html_path_list: list[Path],
    save_dir: Path = RAWDF_DIR,
    save_filename: str = "results.csv"
) -> pd.DataFrame:
    """
    保存済みのレースページのhtmlを読み込んでレース結果テーブルを加工する関数。リスト型をつなげる。
    """
    
    dfs = {} 
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            
            try:
                race_id = html_path.stem  # stemで拡張子なしのファイル名を取得
                html = f.read()
                # if not html.strip():
                #     print(f"File {html_path} is empty or corrupted")
                # else:
                #     print(f"HTML content for {html_path}:\n{html[:500]}")  # 内容を部分的に確認
                soup = BeautifulSoup(html, "lxml").find("table",class_="race_table_01 nk_tb_common")

                df = pd.read_html(html)[0]
                
                # horse_id列を追加
                a_list = soup.find_all("a", href=re.compile(r"/horse/"))
                horse_id_list = []
                for a in a_list:
                    horse_id = re.findall(r"\d{10}",a["href"])[0]
                    # print(horse_id)
                    horse_id_list.append(horse_id)
                df["horse_id"] = horse_id_list

                # jockey_id列追加
                a_list = soup.find_all("a", href=re.compile(r"/jockey/"))
                jockey_id_list = []
                for a in a_list:
                    jockey_id = re.findall(r"\d{5}",a["href"])[0]
                    jockey_id_list.append(jockey_id)
                df["jockey_id"] = jockey_id_list

                # trainer_id列追加
                trainer_id_list = []
                a_list = soup.find_all("a", href=re.compile(r"/trainer/"))
                for a in a_list:
                    trainer_id = re.findall(r"\d{5}",a["href"])[0]
                    trainer_id_list.append(trainer_id)
                df["trainer_id"] = trainer_id_list

                # owner_id列追加
                owner_id_list = []
                a_list = soup.find_all("a", href=re.compile(r"/owner/"))
                for a in a_list:
                    owner_id = re.findall(r"\d{6}",a["href"])[0]
                    owner_id_list.append(owner_id)
                df["owner_id"] = trainer_id_list
                
                df.index = [race_id]*len(df)
                dfs[race_id] = df
            except:
                print(f"Error processing {html_path}") 
                print(traceback.format_exc()) 
    concat_df = pd.concat(dfs.values())
    concat_df.index.name = "race_id"
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    concat_df.to_csv(save_dir /save_filename, sep="\t")
    return concat_df




def table_horse_results(
    html_path_list: list[Path],
    save_dir: Path = RAWDF_DIR,
    save_filename: str = "horse_results.csv"
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
                df.index = [horse_id]*len(df)
                dfs[horse_id] = df
            except:
                print(f"Error processing {html_path}") 
                print(traceback.format_exc()) 
    concat_df = pd.concat(dfs.values())
    concat_df.index.name = "horse_id"
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    concat_df.to_csv(save_dir /save_filename, sep="\t")
    return concat_df
