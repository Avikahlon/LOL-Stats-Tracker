from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from botocore.exceptions import ClientError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from dotenv import load_dotenv
sys.path.append('../../')
from helper import get_teams, data_cleaner

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

    team_data = get_teams()

    upload_file_to_s3(data=team_data, bucket=BUCKET_NAME, object_name='teams.json', prefix='teams')


def transform_data(**kwargs):

    team_data = get_file_from_s3(BUCKET_NAME, object_name='teams.json', prefix='teams')
    teams_list = []
    print(len(team_data))

    for t in team_data:
        team = {
            "name": t.get("name", ""),
            "region": t.get("region", ""),
            "games": int(t["games"]) if t.get("games") not in [None, "", "-"] else None,
            "gpm": int(t["GPM"]) if t.get("GPM") not in [None, "", "-"] else None,
            "gdm": int(t["GDM"]) if t.get("GDM") not in [None, "", "-"] else None,
            "game_duration": int(str(t.get("gameDuration", "0")).replace(":", "")) if t.get("gameDuration") not in [None, "", "-"] else None,
            "winrate": float(str(t.get("winrate", "0")).replace("%", "")) if t.get("winrate") not in [None, "", "-"] else None,
            "kda": float(t.get("k:d")) if t.get("k:d") not in [None, "", "-"] else None,
            "kills_per_game": float(t.get("killsPerGame")) if t.get("killsPerGame") not in [None, "", "-"] else None,
            "deaths_per_game": float(t.get("deathsPerGame")) if t.get("deathsPerGame") not in [None, "", "-"] else None,
            "towers_killed": float(t.get("towersKilled")) if t.get("towersKilled") not in [None, "", "-"] else None,
            "towers_lost": float(t.get("towersLost")) if t.get("towersLost") not in [None, "", "-"] else None,
            "fb_pct": float(str(t.get("FBpercent")).replace("%", "")) if t.get("FBpercent") not in [None, "", "-"] else None,
            "ft_pct": float(str(t.get("FTpercent")).replace("%", "")) if t.get("FTpercent") not in [None, "", "-"] else None,
            "fos_pct": float(t.get("FOSpercent").replace("%", "")) if t.get("FOSpercent") not in [None, "", "-"] else None,
            "drags_per_game": float(t.get("dragsPerGame")) if t.get("dragsPerGame") not in [None, "", "-"] else None,
            "drag_pct": float(str(t.get("dragPercent")).replace("%", "")) if t.get("dragPercent") not in [None, "", "-"] else None,
            "vg_per_game": float(t.get("vgPerGame")) if t.get("vgPerGame") not in [None, "", "-"] else None,
            "herald_pct": float(str(t.get("heraldPercent")).replace("%", "")) if t.get("heraldPercent") not in [None, "", "-"] else None,
            "atak_pct": float(str(t.get("atakPercent")).replace("%", "")) if t.get("atakPercent") not in [None, "", "-"] else None,
            "avg_drags15": float(t.get("avgDrags15")) if t.get("avgDrags15") not in [None, "", "-"] else None,
            "td_at15": float(t.get("TDat15")) if t.get("TDat15") not in [None, "", "-"] else None,
            "gd_at15": float(t.get("GDat15")) if t.get("GDat15") not in [None, "", "-"] else None,
            "plates_per_game": float(t.get("platesPerGame")) if t.get("platesPerGame") not in [None, "", "-"] else None,
            "baron_per_game": float(t.get("baronPergame")) if t.get("baronPergame") not in [None, "", "-"] else None,
            "baron_pct": float(str(t.get("baronPercent")).replace("%", "")) if t.get("baronPercent") not in [None, "", "-"] else None,
            "cspm": float(t.get("cspm")) if t.get("cspm") not in [None, "", "-"] else None,
            "dpm": float(t.get("dpm")) if t.get("dpm") not in [None, "", "-"] else None,
            "wpm": float(t.get("wpm")) if t.get("wpm") not in [None, "", "-"] else None,
            "vision_wards_pm": float(t.get("visionWardsPM")) if t.get("visionWardsPM") not in [None, "", "-"] else None,
            "wards_cleared_pm": float(t.get("wardsClearedPM")) if t.get("wardsClearedPM") not in [None, "", "-"] else None,
            "season": t.get("season", ""),
            "split": t.get("split", "")
        }

        print(f"Adding team: {team['name']} ({team['region']})")
        teams_list.append(team)

    
    data = [
    (
        t["name"], t["region"], t["games"], t["winrate"], t["kda"], t["gpm"], t["gdm"],
        t["game_duration"], t["kills_per_game"], t["deaths_per_game"], t["towers_killed"],
        t["towers_lost"], t["fb_pct"], t["ft_pct"], t["fos_pct"], t["drags_per_game"],
        t["drag_pct"], t["vg_per_game"], t["herald_pct"], t["atak_pct"], t["avg_drags15"],
        t["td_at15"], t["gd_at15"], t["plates_per_game"], t["baron_per_game"], t["baron_pct"],
        t["cspm"], t["dpm"], t["wpm"], t["vision_wards_pm"], t["wards_cleared_pm"],
        t["season"], t["split"]
    )
    for t in teams_list
]

    query = """
            INSERT INTO teams_staging (
                name, region, games, winrate, kda, gpm, gdm, game_duration,
                kills_per_game, deaths_per_game, towers_killed, towers_lost,
                fb_pct, ft_pct, fos_pct, drags_per_game, drag_pct, vg_per_game,
                herald_pct, atak_pct, avg_drags15, td_at15, gd_at15, plates_per_game,
                baron_per_game, baron_pct, cspm, dpm, wpm, vision_wards_pm,
                wards_cleared_pm, season, split
            )
            VALUES %s
            ON CONFLICT (name, region, season, split) DO UPDATE SET
                games = EXCLUDED.games,
                winrate = EXCLUDED.winrate,
                kda = EXCLUDED.kda,
                gpm = EXCLUDED.gpm,
                gdm = EXCLUDED.gdm,
                game_duration = EXCLUDED.game_duration,
                kills_per_game = EXCLUDED.kills_per_game,
                deaths_per_game = EXCLUDED.deaths_per_game,
                towers_killed = EXCLUDED.towers_killed,
                towers_lost = EXCLUDED.towers_lost,
                fb_pct = EXCLUDED.fb_pct,
                ft_pct = EXCLUDED.ft_pct,
                fos_pct = EXCLUDED.fos_pct,
                drags_per_game = EXCLUDED.drags_per_game,
                drag_pct = EXCLUDED.drag_pct,
                vg_per_game = EXCLUDED.vg_per_game,
                herald_pct = EXCLUDED.herald_pct,
                atak_pct = EXCLUDED.atak_pct,
                avg_drags15 = EXCLUDED.avg_drags15,
                td_at15 = EXCLUDED.td_at15,
                gd_at15 = EXCLUDED.gd_at15,
                plates_per_game = EXCLUDED.plates_per_game,
                baron_per_game = EXCLUDED.baron_per_game,
                baron_pct = EXCLUDED.baron_pct,
                cspm = EXCLUDED.cspm,
                dpm = EXCLUDED.dpm,
                wpm = EXCLUDED.wpm,
                vision_wards_pm = EXCLUDED.vision_wards_pm,
                wards_cleared_pm = EXCLUDED.wards_cleared_pm

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
                        CREATE TABLE IF NOT EXISTS teams_staging (
                        name VARCHAR(255),
                        region VARCHAR(10),
                        games INTEGER,
                        winrate FLOAT,
                        kda FLOAT,
                        gpm INTEGER,
                        gdm INTEGER,
                        game_duration INTEGER,
                        kills_per_game FLOAT,
                        deaths_per_game FLOAT,
                        towers_killed FLOAT,
                        towers_lost FLOAT,
                        fb_pct FLOAT,
                        ft_pct FLOAT,
                        fos_pct FLOAT,
                        drags_per_game FLOAT,
                        drag_pct FLOAT,
                        vg_per_game FLOAT,
                        herald_pct FLOAT,
                        atak_pct FLOAT,
                        avg_drags15 FLOAT,
                        td_at15 FLOAT,
                        gd_at15 FLOAT,
                        plates_per_game FLOAT,
                        baron_per_game FLOAT,
                        baron_pct FLOAT,
                        cspm FLOAT,
                        dpm FLOAT,
                        wpm FLOAT,
                        vision_wards_pm FLOAT,
                        wards_cleared_pm FLOAT,
                        season VARCHAR(10),
                        split VARCHAR(10),
                        PRIMARY KEY (name, region, season, split)
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
    dag_id="team_etl_pipeline",
    default_args=default_args,
    description='An ETL pipeline to extract, transform and load teams data',
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