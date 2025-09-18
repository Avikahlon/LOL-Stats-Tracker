from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
import boto3
import json
import psycopg2
import os
from botocore.exceptions import ClientError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from dotenv import load_dotenv
sys.path.append('../../')
from helper import get_players, data_cleaner

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Get environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

ENDPOINT = os.getenv('endpoint')
PORT = int(os.getenv('port', 5432))
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DBNAME = os.getenv('dbname')
REGION = os.getenv('region')

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
    """Upload data to an S3 bucket

    :param data: Data to upload - can be either a file path (string) or JSON data (dict)
    :param bucket: Bucket to upload to
    :param object_name: Name to give the S3 object
    :param prefix: Optional prefix (folder) in the bucket (default: '')
    :return: True if data was uploaded, else False
    """
    # Construct the full S3 key with prefix
    full_key = f"{prefix.rstrip('/')}/{object_name}" if prefix else object_name
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=full_key,
            Body=data
        )
        print("Successfully uploaded data to S3")
    except ClientError as e:
        print(f"Failed to upload data to S3: {e}")
        raise e


def get_file_from_s3(bucket, object_name, prefix=''):
    """Get data from an S3 bucket

    :param bucket: Bucket to get data from
    :param object_name: S3 object name
    :param prefix: Optional prefix (folder) in the bucket (default: '')
    :return: The data from S3 (as dictionary if JSON, raw data otherwise)
    :raises: ClientError if the file cannot be retrieved
    """
    # Construct the full S3 key with prefix
    full_key = f"{prefix.rstrip('/')}/{object_name}" if prefix else object_name

    try:
        # Get the object from S3
        response = s3.get_object(Bucket=bucket, Key=full_key)
        data = response['Body'].read().decode('utf-8')
        
        # Try to parse as JSON, return raw data if not JSON
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
            
    except ClientError as e:
        print(f"Failed to get file from S3: {e}")
        raise e


def extract_data(**kwargs):

    player_data = get_players()

    upload_file_to_s3(data=player_data, bucket=BUCKET_NAME, object_name='players.json', prefix='players')


def transform_data(**kwargs):

    player_data = get_file_from_s3(BUCKET_NAME, object_name='players.json', prefix='players')
    players_list = []

    for p in player_data:
        player = {
            "name": data_cleaner(p.get("name", ""), str),
            "link": data_cleaner(p.get("link", ""), str),
            "country": data_cleaner(p.get("country", ""), str),
            "games": data_cleaner(p.get("games", 0), int),
            "winrate": data_cleaner(str(p.get("winrate", "")).strip(" %"), float),
            "kda": data_cleaner(p.get("kda", 0), float),
            "avg_kills": data_cleaner(p.get("avg_kills", 0), float),
            "avg_deaths": data_cleaner(p.get("avg_deaths", 0), float),
            "avg_assists": data_cleaner(p.get("avg_assists", 0), float),
            "csm": data_cleaner(p.get("csm", 0), float),
            "gpm": data_cleaner(p.get("gpm", 0), float),
            "kp": data_cleaner(str(p.get("kp", "")).strip("%"), float),
            "dmg_pct": data_cleaner(p.get("dmg_pct", 0), float),
            "dpm": data_cleaner(p.get("dpm", 0), float),
            "vspm": data_cleaner(p.get("vspm", 0), float),
            "wpm": data_cleaner(p.get("wpm", 0), float),
            "wcpm": data_cleaner(p.get("wcpm", 0), float),
            "vwpm": data_cleaner(p.get("vwpm", 0), float),
            "gd15": data_cleaner(p.get("gd15", 0), float),
            "csd15": data_cleaner(p.get("csd15", 0), float),
            "xpd15": data_cleaner(p.get("xpd15", 0), float),
            "fb_pct": data_cleaner(str(p.get("fb_pct", "")).strip(" %"), float),
            "fb_victim_pct": data_cleaner(str(p.get("fb_victim_pct", "")).strip(" %"), float),
            "penta_kills": data_cleaner(p.get("penta_kills", 0), int),
            "solo_kills": data_cleaner(p.get("solo_kills", 0), int),
            "season": data_cleaner(p.get("season", ""), str),
            "split": data_cleaner(p.get("split", ""), str),
        }

        players_list.append(player)
    
    data = [
    (
        p["name"], p["link"], p["country"], p["games"], p["winrate"], p["kda"],
        p["avg_kills"], p["avg_deaths"], p["avg_assists"], p["csm"], p["gpm"], p["kp"],
        p["dmg_pct"], p["dpm"], p["vspm"], p["wpm"], p["wcpm"], p["vwpm"],
        p["gd15"], p["csd15"], p["xpd15"], p["fb_pct"], p["fb_victim_pct"],
        p["penta_kills"], p["solo_kills"], p["season"], p["split"]
    )
    for p in players_list]

    query = """
            INSERT INTO players_staging (
                name, link, country, games, winrate, kda,
                avg_kills, avg_deaths, avg_assists, csm, gpm, kp,
                dmg_pct, dpm, vspm, wpm, wcpm, vwpm,
                gd15, csd15, xpd15, fb_pct, fb_victim_pct,
                penta_kills, solo_kills, season, split
            )
            VALUES %s
            ON CONFLICT (name, season, split) DO UPDATE
            SET games = EXCLUDED.games,
                winrate = EXCLUDED.winrate,
                kda = EXCLUDED.kda,
                avg_kills = EXCLUDED.avg_kills,
                avg_deaths = EXCLUDED.avg_deaths,
                avg_assists = EXCLUDED.avg_assists,
                gpm = EXCLUDED.gpm,
                kp = EXCLUDED.kp,
                dmg_pct = EXCLUDED.dmg_pct,
                dpm = EXCLUDED.dpm
            """

    try:
        conn = psycopg2.connect(
                                host=ENDPOINT, 
                                port=PORT, 
                                database=DBNAME, 
                                user=USER, 
                                password=PASSWORD
                            )
        cur = conn.cursor()
        cur.execute("""
                        CREATE TABLE IF NOT EXISTS players_staging (
                        name VARCHAR(255),
                        link TEXT,
                        country VARCHAR(100),
                        games INTEGER,
                        winrate FLOAT,
                        kda FLOAT,
                        avg_kills FLOAT,
                        avg_deaths FLOAT,
                        avg_assists FLOAT,
                        csm FLOAT,
                        gpm FLOAT,
                        kp FLOAT,
                        dmg_pct FLOAT,
                        dpm FLOAT,
                        vspm FLOAT,
                        wpm FLOAT,
                        wcpm FLOAT,
                        vwpm FLOAT,
                        gd15 FLOAT,
                        csd15 FLOAT,
                        xpd15 FLOAT,
                        fb_pct FLOAT,
                        fb_victim_pct FLOAT,
                        penta_kills INTEGER,
                        solo_kills INTEGER,
                        season VARCHAR(10),
                        split VARCHAR(10),
                        PRIMARY KEY (name, season, split)
                        )
                    """)
        conn.commit()
        cur = conn.cursor()
        execute_values(cur, query, data, page_size=500)
        conn.commit()
        print("Data inserted successfully")
    except Exception as e:
        print("Database connection failed due to {}".format(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
                
#this function will load data from staging table to main table
def load_data(**kwargs):
    print("Done")

with DAG(
    dag_id="player_etl_pipeline",
    default_args=default_args,
    description='An ETL pipeline to extract, transform and load player data',
    start_date=datetime(2025, 9, 3),
    schedule='@daily',
    catchup=False,
) as dag:
    task_generate = PythonOperator(
        task_id='scrape_data',
        python_callable=extract_data,
    )
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    task_upload = PythonOperator(
        task_id='print',
        python_callable=load_data,

    )

    task_generate >> task_transform >> task_upload