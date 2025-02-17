import json
import pandas as pd
from pathlib import Path

COMMON_DATA_DIR = Path("..","..","common","data")
RAWDF_DIR = COMMON_DATA_DIR/"rawdf"
POPULATION_DIR = Path("..","data","00_population")
MAPPING_DIR = COMMON_DATA_DIR/"mapping"
OUTPUT_DIR = Path("..","data","01_preprocessed")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

#カテゴリ変数を数値に変換するためのマッピング
with open(MAPPING_DIR/"sex.json","r") as f:
    sex_mapping = json.load(f)
with open(MAPPING_DIR/"race_type.json", "r") as f:
    race_type_mapping = json.load(f)
with open(MAPPING_DIR/"weather.json", "r") as f:
    weather_mapping = json.load(f)  
with open(MAPPING_DIR/"ground_condition.json", "r") as f:
    ground_condition_mapping = json.load(f)
with open(MAPPING_DIR/"race_class.json", "r") as f:
    race_class_mapping = json.load(f)
with open(MAPPING_DIR/"place.json", "r") as f:
    place_mapping = json.load(f)
with open(MAPPING_DIR/"around.json", "r") as f:
    around_mapping = json.load(f)


def process_results(
    population_dir: Path = POPULATION_DIR,
    population_filename: str = "population.csv",
    input_dir: Path = RAWDF_DIR,
    output_dir: Path = OUTPUT_DIR,
    input_filename: str = "results.csv",
    output_filename: str = "results.csv",
    sex_mapping: dict = sex_mapping,
) ->pd.DataFrame:
    

    """
    レース結果テーブルのrawデータファイルをinput_dirから読み込んで加工しoutput_dirに保存する関数
    """

    population = pd.read_csv(population_dir / population_filename, sep="\t")
    df = (
        pd.read_csv(input_dir / input_filename, sep="\t")
    # .query("race_id in @population['race_id']")
    )
    df = df[df['race_id'].isin(population['race_id'])]
    df["rank"] = pd.to_numeric(df["着順"], errors="coerce")
    df.dropna(subset=["rank"],inplace=True)
    df["rank"] = df["rank"].astype(int)
    df["umaban"] = df["馬番"].astype(int)
    df["wakuban"] = df["枠番"].astype(int)
    df["sex"] = df["性齢"].str[0].map(sex_mapping)
    df["age"] = df["性齢"].str[1:].astype(int)
    df["kinryou"] = df["斤量"].astype(float)
    df["weight"] = df["馬体重"].str.extract(r"(\d+)").astype(int)
    df["weight"] = pd.to_numeric(df["weight"],errors = "coerce")
    df["weight_diff"] = df["馬体重"].str.extract(r"\((.+)\)").astype(int)
    df["weight_diff"] = pd.to_numeric(df["weight_diff"],errors = "coerce")
    df["tansyo_odds"] = df["単勝"].astype(float)
    df["popularity"] = df["人気"].astype(int)
    
    # データが着順のとおりに並んでいることで処理がリークしてしまう可能性があるので各レースを馬番順にソートする
    df = df.sort_values(["race_id", "umaban"])
    
    # 使用する列を選択
    df = df[
        [
            "race_id",
            "horse_id",
            "jockey_id",
            "trainer_id",
            "owner_id",
            "rank",
            "umaban",
            "wakuban",
            "tansyo_odds",
            "popularity",
            "kinryou",
            "sex",
            "age",
            "weight",
            "weight_diff",
        ]
    ]
    
    df.to_csv(output_dir / output_filename, sep = "\t" , index =False)
    return df


def process_horse_results(
    POPULATION_DIR: Path = POPULATION_DIR,
    POPULATION_FILENAME: str = "population.csv",
    input_dir: Path = RAWDF_DIR,
    output_dir: Path = OUTPUT_DIR,
    input_filename: str = "horse_results.csv",
    output_filename: str = "horse_results.csv",
    race_type_mapping: dict = race_type_mapping,
    weather_mapping: dict = weather_mapping,
    ground_condition_mapping: dict = ground_condition_mapping,
    race_class_mapping:dict = race_class_mapping,
    
) ->pd.DataFrame:
    
    """
    馬の過去成績テーブルをinput_dirから読み込み加工してoutput_dirに保存する関数
    """

    population = pd.read_csv(POPULATION_DIR / POPULATION_FILENAME, sep="\t")
    df = (
        pd.read_csv(input_dir/input_filename, sep="\t")
        # .query( "horse_id in @population['horse_id']" )
    )
    df = df[df['horse_id'].isin(population['horse_id'])]
    df["rank"] = pd.to_numeric(df["着順"], errors="coerce")
    df.dropna(subset=["rank"],inplace=True)
    df["date"] = pd.to_datetime(df["日付"])
    df["weather"] = df["天気"].map(weather_mapping)
    df["race_type"] = df["距離"].str[0].map(race_type_mapping)
    df["course_len"] = df["距離"].str.extract(r"(\d+)").astype(int)
    df["ground_condition"] = df["馬場"].map(ground_condition_mapping)
    df["rank_diff"] = df["着差"].map(lambda x: 0 if x<0 else x)
    df["prize"] = df["賞金"].fillna(0)
    regex_race_class = "|".join(race_class_mapping)
    df["race_class"] = (
        df["レース名"].str.extract(rf"({regex_race_class})")[0].map(race_class_mapping)
    )
    df.rename(columns={"頭数":"n_horses"}, inplace=True)
    
    # 使用する列を選択
    df = df[
        [
            "horse_id",
            "date",
            "rank",
            "prize",
            "rank_diff",
            "weather",
            "race_type",
            "course_len",
            "ground_condition",
            "race_class",
            "n_horses",
        ]
    ]
    
    df.to_csv(output_dir / output_filename, sep="\t", index=False)
    return df


def process_race_info(
    POPULATION_DIR: Path = POPULATION_DIR,
    POPULATION_FILENAME: str = "population.csv",
    input_dir: Path = RAWDF_DIR,
    output_dir: Path = OUTPUT_DIR,
    input_filename: str = "race_info.csv",
    output_filename: str= "race_info.csv",
    race_type_mapping: dict = race_type_mapping,
    around_mapping: dict = around_mapping,
    weather_mapping: dict = weather_mapping,
    ground_state_mapping: dict = ground_condition_mapping,
    race_class_mapping: dict = race_class_mapping,
    
) -> pd.DataFrame:
    
    """
    未加工のレース情報テーブルのrawデータファイルをinput_dirから読み込んで加工しoutput_dirに保存する関数
    """
    population = pd.read_csv(POPULATION_DIR / POPULATION_FILENAME, sep="\t")
    df = (
        pd.read_csv(input_dir / input_filename, sep="\t")
        # .query("[race_id] in @population['race_id']" )
    )
    df = df[df['race_id'].isin(population['race_id'])]
    # evalで文字列型の列をリスト型に変換し、一時的な列を作成
    df["tmp"] = df["info_1"].map(lambda x: eval(x)[0])
    # ダートor芝or障害
    df["race_type"] = df["tmp"].str[0].map(race_type_mapping)
    # 右or左or直線
    df["around"] = df["tmp"].str[1].map(around_mapping)
    df["course_len"] = df["tmp"].str.extract(r"(\d+)")
    df["weather"] = df["info_1"].str.extract(r"天候:(\w+)")[0].map(weather_mapping)
    df["ground_state"] = (
        df["info_1"].str.extract(r"(芝|ダート|障害):(\w+)")[1].map(ground_state_mapping)
    )
    df["date"] = pd.to_datetime(df["info_2"].map(lambda x: eval(x)[0]), format="%Y年%m月%d日" )
    regex_race_class = "|".join(race_class_mapping)
    df["race_class"] = (
        df["title"].str.extract(rf"({regex_race_class})")
        # タイトルからレース階級情報が取れない場合はinfo_2から取得
        .fillna(df["info_2"].str.extract(rf"({regex_race_class})"))[0]
        .map(race_class_mapping)
    )
    df["place"] = df["race_id"].astype(str).str[4:6].astype(int)
    

    # 使用する列を選択
    df = df[
        [
            "race_id",
            "date",
            "race_type",
            "around",
            "course_len",
            "weather",
            "ground_state",
            "race_class",
            "place",
           
        ]
    ]
    df.to_csv(output_dir / output_filename, sep="\t", index=False)
    return df



def process_return_tables(
    POPULATION_DIR: Path = POPULATION_DIR,
    POPULATION_FILENAME: str = "population.csv",
    input_dir: Path = RAWDF_DIR,
    output_dir: Path = OUTPUT_DIR,
    input_filename: str = "return_tables.csv",
    output_filename: str= "return_tables.pickle",
) -> pd.DataFrame:
    
    """
    未加工の払い戻しテーブルのrawデータファイルをinput_dirから読み込んで加工し
    output_dirに保存する関数
    """
    population = pd.read_csv(POPULATION_DIR / POPULATION_FILENAME, sep="\t")
    df = (
        pd.read_csv(input_dir / input_filename, sep="\t", index_col=0)
        # .query("[race_id] in @population['race_id']")
    )
    df = df[df['race_id'].isin(population['race_id'])]
    df = df.loc[:,["0","1","2"]]
    df = (
        df[["0","1","2"]]
        .replace(" (-|→) ","-", regex=True)
        .apply(lambda x: x.str.split())
        .explode(["1","2"])
        .explode("0")
        .apply(lambda x: x.str.split("-"))
        .explode(["0","2"])
        .replace(",","", regex=True)
    )
    df.columns = ["bet_type", "win_umaban", "return"]
    df["return"] = df["return"].astype(int)
    # リスト型を維持する必要があるためpickleに保存。csvだとリストのかっこの部分が文字型に置き換わってしまうため
    df.to_pickle(output_dir / output_filename)
    return df