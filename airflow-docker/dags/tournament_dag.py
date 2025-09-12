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
from helper import get_tournaments, data_cleaner, format_time

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

    data = get_tournaments()

    json_data = json.dumps(data, indent=4)

    upload_file_to_s3(data=json_data, bucket=BUCKET_NAME, object_name='tournaments.json', prefix='tournaments')


def transform_data(**kwargs):

    data = get_file_from_s3(BUCKET_NAME, object_name='tournaments.json', prefix='tournaments')
    tournaments_list = []

    for season, tournaments in data.items():
        for t in tournaments:
            tournament = {
                'name': data_cleaner(t.get('trname', '').strip(), str),
                'region': data_cleaner(t.get('region', '').strip(), str),
                'number_of_games': data_cleaner((t.get('nbgames', 0)), int),
                'game_duration': data_cleaner((format_time(t.get('avgtime', '')).strip()), int),
                'first_game': data_cleaner(t.get('firstgame', '').strip(), str),
                'last_game': data_cleaner(t.get('lastgame', '').strip(), str),
                'season': data_cleaner(season, str)
            }
            tournaments_list.append(tournament)
    
    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD)
        cur = conn.cursor()
        cur.execute("""
                        CREATE TABLE IF NOT EXISTS tournament_staging (
                        tournament_name VARCHAR(255) PRIMARY KEY,
                        region VARCHAR(10),
                        first_game DATE,
                        last_game DATE,
                        number_of_games INTEGER,
                        game_duration INTEGER,
                        season VARCHAR(10)
                        )
                    """)
        conn.commit()
        for t in tournaments_list:
            cur.execute("""
                            INSERT INTO tournament_staging
                            (tournament_name, region, first_game, last_game, number_of_games, game_duration, season)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (tournament_name) DO UPDATE SET
                            last_game = EXCLUDED.last_game,
                            number_of_games = EXCLUDED.number_of_games,
                            game_duration = EXCLUDED.game_duration,

                            """, (
                            t['name'], t['region'], t['first_game'], t['last_game'], 
                            t['number_of_games'], t['game_duration'], t['season']
                        ))
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
    dag_id="tournament_etl_pipeline",
    default_args=default_args,
    description='An ETL pipeline to extract, transform and load tournament data',
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