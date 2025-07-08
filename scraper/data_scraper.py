import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
import boto3
from datetime import datetime
import csv
from urllib.parse import quote
import re
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import json
from prettytable import PrettyTable
from helper import *



load_dotenv(dotenv_path=".env")

ak = os.getenv("keyid")
sk = os.getenv("key")
region = os.getenv("region")

host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    print("Connected to PostgreSQL!")

    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cur.fetchall()
    print("Tables:", tables)

    # Always close
    cur.close()
    conn.close()

except Exception as e:
    print("Error connecting to database:", e)
    

def data_to_table(data):
    df = pd.DataFrame.from_dict(data)
    return df


# # ---------- Get All Tournaments ----------
# def get_matching_tournaments():
#     url = "https://gol.gg/tournament/ajax.trlist.php"
#     headers = {
#         "accept": "application/json, text/javascript, */*; q=0.01",
#         "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
#         "x-requested-with": "XMLHttpRequest",
#         "referer": "https://gol.gg/tournament/list/",
#         "cookie": "PHPSESSID=tpl9ud9tgc3tbtnvpcpmskavo9",
#     }

#     data = {
#         "season": "S15"
#     }

#     keywords = ["LCK", "LPL", "LEC", "NLC"]
#     response = requests.post("https://gol.gg/tournament/ajax.trlist.php", data={"season": "S15"})

#     tournaments = response.json()
#     filtered_tournaments = [
#         t for t in tournaments
#         if t["trname"].split()[0].lower() in [k.lower() for k in keywords]
#     ]
#     tournament_df = pd.DataFrame(filtered_tournaments)
#     tournament_df = tournament_df[["trname"]]
#     tournament_df.to_csv("tourn.csv", index=False, encoding="utf-8")

#     return tournament_df

# def scrape_tournament(url_suffix, tournament_name):
#     base_url = "https://gol.gg/players/list/season-S15/split-ALL/"
#     url = base_url + url_suffix
#     headers = { "User-Agent": "Mozilla/5.0" }

#     response = requests.get(url, headers=headers)
#     soup = BeautifulSoup(response.text, "html.parser")
#     table = soup.find("table", class_="playerslist")
#     if not table:
#         print(f"No player table found for {tournament_name}")
#         return pd.DataFrame()

#     headers_list = ["Player"] + [clean_text(th.text.strip()) for th in table.find("tr").find_all("th")[1:]]
#     data = []

#     for row in table.find_all("tr")[1:]:
#         cols = row.find_all("td")
#         player_name = cols[0].find("a").text.strip()
#         stats = stats = [clean_text(td.text.strip()) for td in cols[1:]]
#         data.append([player_name] + stats)

#     df = pd.DataFrame(data, columns=headers_list)
#     return df

# def scrape_and_save_all(tournaments_df):
#     all_dfs = {}

#     for _, row in tournaments_df.iterrows():
#         trname = row['trname']
#         url_suffix = "tournament-" + quote(trname) + "/"
#         print(f"Scraping tournament: {trname}")
#         player_df = scrape_tournament(url_suffix, trname)

#         if not player_df.empty:
#             filename = sanitize_filename(trname) + ".csv"
#             try:
#                 s3_filename = upload_dataframe_to_s3(player_df, "lol-stats-data", filename)
#                 print(f"Successfully uploaded to S3 as {s3_filename}")
#                 all_dfs[trname] = player_df
#             except Exception as e:
#                 print(f"Error uploading to S3: {str(e)}")
#                 continue
#         else:
#             print(f"No data found for {trname}")

#     return all_dfs

if __name__ == "__main__":
    # data = get_tournaments()
    # data = get_players()
    data = get_teams()
    d = data_to_table(data)
    print(d[d["k:d"]=="1.5"]) 