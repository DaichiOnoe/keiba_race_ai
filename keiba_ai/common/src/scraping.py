from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm.notebook import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import traceback
from pathlib import Path

DATA_DIR = Path("..", "data")
HTML_DIR =  Path("..","data","html")
HTML_RACE_DIR = HTML_DIR /"race_test"
HTML_HORSE_DIR = HTML_DIR /"horse"
HTML_PED_DIR = HTML_DIR /"ped"
HTML_LEADING_DIR = DATA_DIR / "html"/ "leading"


USER_AGENTS = [
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.0.0",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
]


def scrape_kaisai_date(from_, to_):

    """
    期間を年月で入力すろと、その間に行われたレースが行われた開催日を返してくれるコード。
    """
    
    kaisai_date_list = []
    for date in tqdm(pd.date_range(from_, to_, freq="MS")):
        year = date.year
        month = date.month
        url = f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
        # Requestオブジェクトを作成
        req = Request(url, headers=headers)
        # リクエストを送信 # URLのHTMLを取得 #スクレイピング
        html = urlopen(req).read()
        time.sleep(1) #サーバーに負荷をかけないようにする(for文とスクレイピングが出てきたときは注意する)
        #htmlを加工
        soup = BeautifulSoup(html, "lxml")
        #html文字列から開催日idを取り出す
        a_list = soup.find("table", class_="Calendar_Table").find_all("a")
       
        for a in a_list:
            kaisai_date = re.findall(r"kaisai_date=(\d{8})", a["href"])[0]
            kaisai_date_list.append(kaisai_date)
            
    return kaisai_date_list




def scrape_race_id_list(kaisai_date_list: list[str], save_dir = None) ->list[str]:
    
    """
    開催日(yyyymmdd)をリストで入れると、レースidが返ってくる関数.save_dirを指定するとrace_idがrace_id_list.txtに保存される
    """
    options = Options()
    options.add_argument("--headless") #バックグラウンドで実行
    #最新のChromeDriverを自動的にダウンロードして使用できるが故障しているので廃止中
    # driver_path = ChromeDriverManager().install() 
    # 無理やりpathを指定
    driver_path = 'C:/Users/Onoe Daichi/Downloads/競馬AI/keiba_ai/common/src/chromedriver-win32/chromedriver.exe'
    
    # save_dir.mkdir(parents=True, exist_ok=True)
    # save_filename: str = "race_id_list.txt"
    # save_filepath = save_dir / save_filename
    race_id_list = []
    # 処理が終わったときに自動的にchromeを閉じる関数
    with webdriver.Chrome(service=Service(driver_path), options=options) as driver:
        for kaisai_date in tqdm(kaisai_date_list):
            url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}"
            # 予想外のエラーが起きたときなどにexceptに移る
            try:
                driver.get(url)
                time.sleep(1)
                li_list = driver.find_elements(By.CLASS_NAME, "RaceList_DataItem")
                for li in li_list:
                    href = li.find_element(By.TAG_NAME, "a").get_attribute("href")
                    race_id = re.findall(r"race_id=(\d{12})",href)[0]
                    race_id_list.append(race_id)
            except:
                print(f"stopped at {url}") #どのurlでエラーが起きたのか表示する
                print(traceback.format_exc()) #エラーの内容を表示する
                break
    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)
        with open(save_dir / "race_id_list.txt", "w") as f:
            f.write("\n".join(race_id_list))
    return race_id_list
        

def scrape_html_race(race_id_list: list[str], save_dir: Path = HTML_RACE_DIR, skip: bool = True) -> list[Path]:
    """
    netkeiba.comのraceページのhtmlをスクレイピングしてsave_dirに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされる。
    逆に上書きしたい場合は、skip=Falseにする。
    スキップされたhtmlのパスは返り値に含まれない。
    """
    html_path_list = []
    # save_dir.mkdir(parents=True, exist_ok=True)
    for race_id in tqdm(race_id_list):
        filepath = save_dir / f"{race_id}.bin"
        if skip and filepath.is_file():
            print(f"skipped:{race_id}")
        else:
            url = f'https://db.netkeiba.com/race/{race_id}/'
            # headers = {USER_AGENTS[0]}
            headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
            req = Request(url, headers=headers)
            html = urlopen(req).read()
            time.sleep(1)
            with open(filepath, "wb") as f:
                f.write(html)
            html_path_list.append(filepath)
    return html_path_list



def scrape_html_horse(
    horse_id_list: list[str], save_dir: Path = HTML_HORSE_DIR, skip: bool =True, path: bool = True
):
    """
    horseページのhtmlをスクレイピング。save_dirに.bin形式で保存。すでにファイルが存在し、skip=Trueの場合は処理がスキップされ新規のものがプラスされて帰ってくる。
    その際、path=Trueにするとhtml_path_listにpathは追加されるようになる。
    """
    html_path_list = []
    save_dir.mkdir(parents=True, exist_ok=True)
    for horse_id in tqdm(horse_id_list):
        filepath = save_dir / f"{horse_id}.bin"
        # binファイルが存在し、skip=Trueの場合はスキップする
        if filepath.is_file() and skip:
            print(f"skipped:{horse_id}")
            if path:
                html_path_list.append(filepath)
        else:
            url = f'https://db.netkeiba.com/horse/{horse_id}/'
            headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
            req = Request(url, headers=headers)
            response = urlopen(req)
            html = response.read()
            time.sleep(1)
            with open(filepath, "wb") as f:
                f.write(html)
            html_path_list.append(filepath)
    return html_path_list

def scrape_html_leading(
    leading_type:str,
    year: int,
    pages: list[int],
    save_dir: Path = HTML_LEADING_DIR,
    skip: bool = True,
) -> list[Path]:
    """
    リーディングページのhtmlをスクレイピングしてsave_dirに保存する関数。
    leading_type: str = ["jockey" or "trainer" or "sire" or "bms"]_leadingと指定する。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされる。
    逆に上書きしたい場合は、skip=Falseにする。
    スキップされたhtmlのパスは返り値に含まれない。
    """
    scrape_html_path_list = []
    save_dir = save_dir / leading_type
    save_dir.mkdir(parents=True, exist_ok=True)
    for page in tqdm(pages):
        filepath = save_dir / f"{year}_{str(page).zfill(2)}.bin"
        if skip and filepath.is_file():
            print(f"skipped:{leading_type}_{year}_{page}")
        else:
            url = f"https://db.netkeiba.com/?pid={leading_type}&year={year}&page={page}"
            headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
            req = Request(url, headers=headers)
            response = urlopen(req)
            html = response.read()
            time.sleep(1)
            with open(filepath, "wb") as f:
                f.write(html)
            scrape_html_path_list.append(filepath)
    return scrape_html_path_list


def scrape_html_ped(
    horse_id_list: list[str], save_dir: Path = HTML_PED_DIR, skip: bool =True, path: bool = True
):
    """
    血統ページののhtmlをスクレイピングしてsave_dirに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされる。
    逆に上書きしたい場合は、skip=Falseにする。
    その際、path=Trueにするとhtml_path_listにpathは追加されるようになる。
    """
    upgrade_html_path_list = []
    save_dir.mkdir(parents=True, exist_ok=True)
    for horse_id in tqdm(horse_id_list):
        filepath = save_dir / f"{horse_id}.bin"
        # binファイルが存在し、skip=Trueの場合はスキップする
        if filepath.is_file() and skip:
            print(f"skipped:{horse_id}")
            if path:
                upgrade_html_path_list.append(filepath)
        else:
            url = f'https://db.netkeiba.com/horse/ped/{horse_id}/'
            headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
            req = Request(url, headers=headers)
            html = urlopen(req).read()
            time.sleep(1)
            with open(filepath, "wb") as f:
                f.write(html)
            upgrade_html_path_list.append(filepath)
    return upgrade_html_path_list
