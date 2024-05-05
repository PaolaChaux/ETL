import pandas as pd
import sqlalchemy
import json
import logging
from sqlalchemy import create_engine
import psycopg2
from sodapy import Socrata
from transform_dag import transformations_api_data
from transform_dag import apply_transformations



def read_water():
    with open('db_config.json') as file:
        db_config = json.load(file)

    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:5432/{db_config["dbname"]}')

    water = pd.read_sql('SELECT * FROM water_table', engine)
    
    return water
    


def transform_water(**kwargs):
    ti = kwargs['ti']
    json_data = json.loads(ti.xcom_pull(task_ids='read_water'))
    spotify_df = pd.json_normalize(data=json_data)
  
    spotify_df = transform_spotify(spotify_df)

    logging.info(f"Los datos transformados de Spotify son: {spotify_df}")
    
    return spotify_df.to_json(orient='records')




def extract_api():

    with open('db_config.json') as file:
        db_config = json.load(file)

    engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:5433/{db_config["dbname"]}')
    
    grammy_df = pd.read_sql('SELECT * FROM the_grammy_awards', engine)
    
    return grammy_df.to_json(orient='records')




def transform_api(**kwargs):
    ti = kwargs['ti']
    json_data = json.loads(ti.xcom_pull(task_ids='read_db'))
    grammy_df = pd.json_normalize(data=json_data)
    
    grammy_df = transform_grammy(grammy_df)

    logging.info(f"Los datos transformados son: {grammy_df}")
    
    return grammy_df.to_json(orient='records')



def merge_task(**kwargs):
    ti = kwargs['ti']
    
    spotify_data = json.loads(ti.xcom_pull(task_ids='transform_csv'))
    grammy_data = json.loads(ti.xcom_pull(task_ids='transform_db'))
    
    merged_df = merge_datasets(pd.json_normalize(spotify_data), pd.json_normalize(grammy_data))
    
    logging.info("Fusión de datos completada.")
    
    return merged_df.to_json(orient='records')



def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)

    logging.info("Cargando datos...")
    
    logging.info("Los datos se han cargado en: tracks")