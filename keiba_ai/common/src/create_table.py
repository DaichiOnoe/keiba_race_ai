import pandas as pd
from pathlib import Path
from tqdm.notebook import tqdm
import traceback
from bs4 import BeautifulSoup
import re
# import time


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
                # owner_id_list = []
                # a_list = soup.find_all("a", href=re.compile(r"/owner/"))
                # for a in a_list:
                #     owner_id = re.findall(r"\d{6}",a["href"])[0]
                #     owner_id_list.append(owner_id)
                # df["owner_id"] = owner_id_list
                
                # 最初の列にrace_idを挿入。なぜか列名のコラムとして追加されていなかったので、追加
                df.insert(0, "race_id", race_id)
                dfs[race_id] = df
            except:
                print(f"Error processing {html_path}") 
                print(traceback.format_exc()) 
    concat_df = pd.concat(dfs.values())
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    concat_df.reset_index()
    updating_rawdf(
        concat_df, 
        key="race_id", 
        save_filename=save_filename, 
        save_dir=save_dir
    ) 
    return concat_df


def table_race_info(
    html_path_list: list[Path],
    save_dir: Path = RAWDF_DIR,
    save_filename: str = "race_info.csv",
) -> pd.DataFrame:
    """
    raceページのhtmlを読み込んで、レース情報テーブルに加工する関数。
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                # ファイル名からrace_idを取得
                race_id = html_path.stem
                html = f.read()
                soup = BeautifulSoup(html, "lxml").find("div", class_="data_intro")
                info_dict = {}
                info_dict["title"] = soup.find("h1").text
                p_list = soup.find_all("p")
                info_dict["info_1"] = re.findall(
                    r"[\w:]+", p_list[0].text.replace(" ", "")
                )
                info_dict["info_2"] = re.findall(r"\w+", p_list[1].text)
                df = pd.DataFrame().from_dict(info_dict, orient="index").T
                # 最初の列にrace_idを挿入
                df.insert(0, "race_id", race_id)
                dfs[race_id] = df
            except IndexError as e:
                print(f"table not found at {race_id}")
                continue
            except AttributeError as e:
                print(f"{e} at {race_id}")
                continue
    concat_df = pd.concat(dfs.values())
    concat_df.columns = concat_df.columns.str.replace(" ", "")
    save_dir.mkdir(exist_ok=True, parents=True)
    updating_rawdf(
        concat_df,
        key="race_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
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
        if html_path.stat().st_size == 0:
            print(f"Skip empty files.: {html_path}")
            continue
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
    updating_rawdf(concat_df, key="horse_id", save_filename=save_filename)
    # concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df


def create_return_tables(
    html_path_list: list[Path],
    save_dir: Path = RAWDF_DIR,
    save_filename: str = "return_tables.csv"
) -> pd.DataFrame:
    """
    保存済みのレースページのhtmlを読み込んで払い戻しテーブルを加工する関数。リスト型をつなげる。
    """
    
    dfs = {} 
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                html = f.read()
                df_list =pd.read_html(html)
                df = pd.concat([df_list[1],df_list[2]])
                # ファイル名からrace_idを取得
                race_id = html_path.stem
                # 最初の列にrace_idを挿入
                df.insert(0, "race_id", race_id)
                dfs[race_id] = df
            except:
                print(f"Error processing {html_path}") 
                continue
    concat_df = pd.concat(dfs.values())
    save_dir.mkdir(exist_ok=True, parents=True)
    updating_rawdf(
        concat_df,
        key="race_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    return concat_df

def create_jockey_leading(
        html_path_list: list[Path],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "jockey_leading.csv"
) -> pd.DataFrame:
    """
    保存済みの騎手ページのhtmlを読み込んで騎手のリーディングテーブルを加工する関数。リスト型をつなげる。
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                # ファイル名からrace_idを取得
                page_id = html_path.stem
                html = f.read()
                soup = BeautifulSoup(html, "lxml").find("table", class_="nk_tb_common")
                df = pd.read_html(html)[0]  
                df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
                # jockey_id列追加
                a_list = soup.find_all("a", href=re.compile(r"^/jockey/"))
                jockey_id_list = []
                for a in a_list:
                    jockey_id = re.findall(r"\d+",a["href"])[0]
                    jockey_id_list.append(jockey_id)
                # 最初の列にjockey_idを挿入
                df.insert(0, "jockey_id", jockey_id_list)
                dfs[jockey_id] = df
                # 最初の列にpage_idを挿入
                df.insert(0, "page_id", page_id)
            except IndexError as e:
                print(f"table not found at {page_id}")
                continue
            except AttributeError as e:
                print(f"{e} at {page_id}")
                continue
    concat_df = pd.concat(dfs.values())
    # concat_df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    updating_rawdf(
        concat_df,
        key="page_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    # concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df
    
def create_trainer_leading(
        html_path_list: list[Path],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "trainer_leading.csv"
) -> pd.DataFrame:
    """
    保存済みの調教師ページのhtmlを読み込んで騎手のリーディングテーブルを加工する関数。リスト型をつなげる。
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                # ファイル名からrace_idを取得
                page_id = html_path.stem
                html = f.read()
                soup = BeautifulSoup(html, "lxml").find("table", class_="nk_tb_common")
                df = pd.read_html(html)[0]
                df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
                # trainer_id列追加
                a_list = soup.find_all("a", href=re.compile(r"/trainer/"))
                trainer_id_list = []
                for a in a_list:
                    trainer_id = re.findall(r"\d+", a["href"])[0]
                    trainer_id_list.append(trainer_id)
                # 最初の列にtrainer_idを挿入
                df.insert(0, "trainer_id", trainer_id_list)
                dfs[trainer_id] = df
                # 最初の列にpage_idを挿入
                df.insert(0, "page_id", page_id)
                # dfs[page_id] = df
            except IndexError as e:
                print(f"table not found at {page_id}")
                continue
            except AttributeError as e:
                print(f"{e} at {page_id}")
                continue
            # time.sleep(0.5)
    concat_df = pd.concat(dfs.values())
    # concat_df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    # updating_rawdf(
    #     concat_df,
    #     key="trainer_id",
    #     save_filename=save_filename,
    #     save_dir=save_dir,
    # )
    concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df

def create_sire_leading(
        html_path_list: list[Path],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "sire_leading.csv"
) -> pd.DataFrame:
    """
    保存済みの種牡馬ページのhtmlを読み込んで種牡馬のリーディングテーブルを加工する関数。リスト型をつなげる。
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                # ファイル名からrace_idを取得
                page_id = html_path.stem
                html = f.read()
                soup = BeautifulSoup(html, "lxml").find("table", class_="nk_tb_common")
                df = pd.read_html(html)[0]
                df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
                # sire_id列追加
                a_list = soup.find_all("a", href=re.compile(r"/sire/"))
                pattern = r'/horse/sire/([0-9a-zA-Z]+)/'
                sire_id_list = []
                # for a in a_list:
                #     sire_id = re.findall(r"\d+", a["href"])[0]
                #     sire_id_list.append(sire_id)
                 # 記号交じりのsire_idを取得するために以下のように修正以下のように修正
                for a in a_list:
                    href = a["href"]  # href 属性を取得
                    match = re.search(pattern, href)  # 正規表現で検索   
                    sire_id = match.group(1)  # 種牡馬識別記号を取得
                    sire_id_list.append(sire_id)  # リストに追加
                # 最初の列にsire_idを挿入
                df.insert(0, "sire_id", sire_id_list)
                dfs[sire_id] = df
                # 最初の列にpage_idを挿入
                df.insert(0, "page_id", page_id)
            except IndexError as e:
                print(f"table not found at {page_id}")
                continue
            except AttributeError as e:
                print(f"{e} at {page_id}")
                continue
    concat_df = pd.concat(dfs.values())
     # concat_df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    updating_rawdf(
        concat_df,
        key="sire_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    # concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df

def create_bms_leading(
        html_path_list: list[Path],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "bms_leading.csv"
) -> pd.DataFrame:
    """
    保存済みの母父ページのhtmlを読み込んで母父のリーディングテーブルを加工する関数。リスト型をつなげる。
    """
    dfs = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            try:
                # ファイル名からrace_idを取得
                page_id = html_path.stem
                html = f.read()
                soup = BeautifulSoup(html, "lxml").find("table", class_="nk_tb_common")
                df = pd.read_html(html)[0]
                df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
                # bms_id列追加
                a_list = soup.find_all("a", href=re.compile(r"/sire/"))
                pattern = r'/horse/sire/([0-9a-zA-Z]+)/'
                bms_id_list = []
                # for a in a_list:
                #     bms_id = re.findall(r"\d+", a["href"])[0]
                #     bms_id_list.append(bms_id)
                for a in a_list:
                    href = a["href"]  # href 属性を取得
                    match = re.search(pattern, href)  # 正規表現で検索   
                    bms_id = match.group(1)  # 種牡馬識別記号を取得
                    bms_id_list.append(bms_id)  # リストに追加
                # 最初の列にbms_idを挿入
                df.insert(0, "bms_id", bms_id_list)
                dfs[bms_id] = df
                # 最初の列にpage_idを挿入
                df.insert(0, "page_id", page_id)
            except IndexError as e:
                print(f"table not found at {page_id}")
                continue
            except AttributeError as e:
                print(f"{e} at {page_id}")
                continue
    concat_df = pd.concat(dfs.values())
    # concat_df.columns = ["_".join(col) if col[0] != col[1] else col[0] for col in df.columns]
    concat_df.columns = concat_df.columns.str.replace(" ","")
    save_dir.mkdir(parents=True, exist_ok=True)
    updating_rawdf(
        concat_df,
        key="bms_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    # concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df
                
                

def create_peds(
        html_path_list: list[Path],
        save_dir: Path = RAWDF_DIR,
        save_filename: str = "peds.csv"
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
    save_dir.mkdir(parents=True, exist_ok=True)
    updating_rawdf(
        concat_df,
        key="horse_id",
        save_filename=save_filename,
        save_dir=save_dir,
    )
    # concat_df.to_csv(save_dir / save_filename, sep="\t", index=False)
    return concat_df


def updating_rawdf(
    new_df: pd.DataFrame,
    key: str,
    save_filename: str,
    save_dir: Path = RAWDF_DIR,
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