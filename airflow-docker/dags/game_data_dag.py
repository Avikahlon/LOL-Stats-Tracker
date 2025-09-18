from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import sys
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv
sys.path.append('../../')
from helper import get_game, data_cleaner, get_urls_from_db
import asyncio

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

ENDPOINT = os.getenv('endpoint')
PORT = int(os.getenv('port', 5432))
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DBNAME = os.getenv('dbname')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def upload_file_to_s3(data, bucket, object_name, prefix=None):
    full_key = f"{prefix.rstrip('/')}/{object_name}" if prefix else object_name
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=full_key,
            Body=json.dumps(data, indent=4)
        )
        print(f"Uploaded {object_name} to S3")
    except ClientError as e:
        print(f"Upload failed: {e}")
        raise

def get_file_from_s3(bucket, object_name, prefix=''):
    full_key = f"{prefix.rstrip('/')}/{object_name}" if prefix else object_name
    response = s3.get_object(Bucket=bucket, Key=full_key)
    data = response['Body'].read().decode('utf-8')
    return json.loads(data)

def extract_data(**kwargs):

    game_data, player_data = [], []
    game_urls = get_urls_from_db()
    game_data, player_data = asyncio.run(get_game(game_urls))

    # Upload both sets
    upload_file_to_s3(game_data, BUCKET_NAME, "games.json", prefix="games")
    upload_file_to_s3(player_data, BUCKET_NAME, "players_game.json", prefix="players")

def transform_data(**kwargs):
    game_data = get_file_from_s3(BUCKET_NAME, "games.json", prefix="games")
    player_data = get_file_from_s3(BUCKET_NAME, "players_game.json", prefix="players")

    Prepare team game data
    games_list = [
        (
            g["team"],
            (g["result"]),
            (g["kills"]),
            (g["towers"]),
            (g["dragons"]),
            (g["barons"]),
            (g["gold"]),
            (g["first_blood"]),
            (g["first_tower"]),
            Json(g["dragon_types"]),
            Json(g["bans"]),
            Json(g["picks"]),
            (g["game_url"])
        )
        for g in game_data
    ]

    players_list = [
        (
            (p["game_url"]),
            (p["game_number"]),
            (p["player_name"]),
            (p["team_side"]),
            (p["champion"]),
            (p["kills"]),
            (p["deaths"]),
            (p["assists"]),
            (p["cs"])
        )
        for p in player_data
    ]

    query_games = """
        INSERT INTO games_staging (
            team, result, kills, towers, dragons, barons, gold,
            first_blood, first_tower, dragon_types, bans, picks, game_url
        )
        VALUES %s
        ON CONFLICT (team, game_url) DO UPDATE
        SET kills = EXCLUDED.kills,
            towers = EXCLUDED.towers,
            gold = EXCLUDED.gold,
            result = EXCLUDED.result
    """

    query_players = """
        INSERT INTO players_data_staging (
            game_url, game_number, player_name, team_side, champion,
            kills, deaths, assists, cs
        )
        VALUES %s
        ON CONFLICT (game_url, player_name, champion) DO UPDATE
        SET kills = EXCLUDED.kills,
            deaths = EXCLUDED.deaths,
            assists = EXCLUDED.assists,
            cs = EXCLUDED.cs
    """

    try:
        conn = psycopg2.connect(
            host=ENDPOINT, port=PORT, dbname=DBNAME,
            user=USER, password=PASSWORD
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS games_staging (
                team TEXT,
                result TEXT,
                kills INT,
                towers INT,
                dragons INT,
                barons INT,
                gold TEXT,
                first_blood BOOLEAN,
                first_tower BOOLEAN,
                dragon_types JSONB,
                bans JSONB,
                picks JSONB,
                game_url TEXT,
                PRIMARY KEY (team, game_url)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players_data_staging (
                game_url TEXT,
                game_number INT,
                player_name TEXT,
                team_side TEXT,
                champion TEXT,
                kills INT,
                deaths INT,
                assists INT,
                cs INT,
                PRIMARY KEY (game_url, player_name, champion)
            )
        """)
        conn.commit()

        execute_values(cur, query_games, games_list, page_size=500)
        execute_values(cur, query_players, players_list, page_size=500)
        conn.commit()
        print("Inserted games and players successfully")

    except Exception as e:
        print(f"DB insert failed: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

def load_data(**kwargs):
    print("Load step (staging -> main) placeholder")

with DAG(
    dag_id="game_player_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for game and player stats",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    task_extract >> task_transform >> task_load
