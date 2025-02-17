from bs4 import BeautifulSoup
import re
import time
from tqdm.notebook import tqdm
import traceback
from pathlib import Path
import requests
from urllib.request import Request, urlopen
import pandas as pd
import scraping


POPULATION_DIR = Path("..", "data", "prediction_population")

def scrape_horse_id_list(race_id: str) ->list[str]:
    
    """
    レースidを入れると、馬idがリストで返ってくる関数.
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
            }
    req = Request(url, headers=headers)
    html = urlopen(req).read()
    soup = BeautifulSoup(html, "lxml")
    span_list = soup.find_all("td", class_="HorseInfo")
    horse_id_list = []
    for span in span_list:
        horse_id = re.findall(r"\d{10}", span.find("a")["href"])[0]
        horse_id_list.append(horse_id)
    return horse_id_list  



def create(
    kaisai_date: str,
    save_dir: Path = POPULATION_DIR,
    save_filename: str = "predict_population.csv"
) -> pd.DataFrame:
    """
    開催日(yyyymmdd)を入れると、予測母集団である(date, race_id, horse_id)のデータフレームを返す関数.
    """
    race_id_list = scraping.scrape_race_id_list([kaisai_date])
    
    dfs =  {}
    for race_id in tqdm(race_id_list):
        horse_id_list = scrape_horse_id_list(race_id)
        time.sleep(1)
        df = pd.DataFrame(
            {"date":kaisai_date, "race_id":race_id, "horse_id":horse_id_list}
        )
        dfs[race_id] = df
    concat_df = pd.concat(dfs.values())
    concat_df["date"] = pd.to_datetime(concat_df["date"])
    concat_df.to_csv(save_dir / save_filename, index=False, sep="\t")
    return concat_df
    
