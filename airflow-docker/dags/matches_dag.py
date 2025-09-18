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
import asyncio
sys.path.append('../../')
from helper import get_matches_async, load_tournament_names, data_cleaner, parse_score

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

    tournaments = load_tournament_names()

    match_data = asyncio.run(get_matches_async(tournaments, max_concurrent=30))

    upload_file_to_s3(data=match_data, bucket=BUCKET_NAME, object_name='matches.json', prefix='matches')


def transform_data(**kwargs):

    match_data = get_file_from_s3(BUCKET_NAME, object_name='matches.json', prefix='matches')
    matches_list = []

    for m in match_data:
        score_str = m.get("score", "0 - 0")
        team1_score, team2_score = parse_score(score_str)
        match = {
            "match_name": m.get("match_name", ""),
            "tournament":m.get("tournament", ""),
            "match_url": m.get("match_url", ""),
            "team1": m.get("team1"),
            "team2": m.get("team2", ""),
            "winner": m.get("winner"),
            "loser": m.get("loser"),
            "score": m.get("score"),
            "team1_score": team1_score,
            "team2_score": team2_score,
            "match_type": m.get("match_type"),
            "patch": m.get("patch"),
            "date": m.get("date"),
            "BO": (m.get("BO")),
            "game_urls": m.get("game_urls"),
        }

        matches_list.append(match)
    
    data = [
        (
            m["match_name"], m["tournament"], m["match_url"],
            m["team1"], m["team2"], m["winner"], m["loser"],
            m["score"], m["team1_score"], m["team2_score"],
            m["match_type"], m["patch"], m["date"], m["BO"],
            Json(m["game_urls"])
        )
        for m in matches_list
    ]

    query = """
        INSERT INTO matches (
            match_name, tournament_name, match_url,
            team1, team2, winner, loser,
            score, team1_score, team2_score,
            match_type, patch, date, BO,
            game_urls
        )
        VALUES %s
        ON CONFLICT DO NOTHING
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
            CREATE TABLE IF NOT EXISTS matches (
                match_name TEXT,
                tournament_name TEXT,
                match_url TEXT,
                team1 TEXT,
                team2 TEXT,
                winner TEXT,
                loser TEXT,
                score TEXT,
                team1_score INT,
                team2_score INT,
                match_type TEXT,
                patch TEXT,
                date DATE,
                BO INT,
                game_urls JSONB,
                PRIMARY KEY (match_name, tournament_name, date)
            )
        """)
        conn.commit()

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
    dag_id="matches_etl_pipeline",
    default_args=default_args,
    description='An ETL pipeline to extract, transform and load matches data',
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