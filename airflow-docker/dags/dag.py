from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.append('../../')
from helper import get_tournaments, data_cleaner


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

def extract_data(**kwargs):
    data = get_tournaments()
    #need to change this to save it somewhere and not use xcom
    kwargs['ti'].xcom_push(key='raw_tournaments', value=data)

def transform_data(**kwargs):
    #need to change this to load it somewhere and not use xcom
    data = kwargs['ti'].xcom_pull(key='raw_tournaments', task_ids='scrape_data')
    tournaments_list = []

    for season, tournaments in data.items():
        for t in tournaments:
            tournament = {
                'name': data_cleaner(t.get('trname', '').strip(), str),
                'region': data_cleaner(t.get('region', '').strip(), str),
                'number_of_games': data_cleaner((t.get('nbgames', 0)), int),
                'game_duration': data_cleaner((t.get('avgtime', '').strip()), int),
                'first_game': data_cleaner(t.get('firstgame', '').strip(), str),
                'last_game': data_cleaner(t.get('lastgame', '').strip(), str),
                'season': data_cleaner(season, str)
            }
            tournaments_list.append(tournament)

    kwargs['ti'].xcom_push(key='clean_tournaments', value=tournaments_list)

def load_data(**kwargs):
    tournaments = kwargs['ti'].xcom_pull(key='clean_tournaments', task_ids='transform_data')

    hook = PostgresHook(postgres_conn_id='neon_postgres')

    try:
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for t in tournaments:
                    cursor.execute("""
                        INSERT INTO tournaments 
                        (tournament_name, region, first_game, last_game, number_of_games, game_duration, season)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (tournament_name) DO NOTHING
                        """, (
                        t['name'], t['region'], t['first_game'], t['last_game'], 
                        t['number_of_games'], t['game_duration'], t['season']
                    ))
                conn.commit()
                print("Data inserted successfully")
    except Exception as e:
        print(f"Connection failed: {e}")

    # for t in tournaments:
    #     print(t)

with DAG(
    dag_id="daily_etl_pipeline_with_transform",
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