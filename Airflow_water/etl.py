import pandas as pd
import sqlalchemy
import json
import logging
from sqlalchemy import create_engine
import psycopg2
from sodapy import Socrata
from transform_dag import transformations_api
from transform_dag import transformations_water





def read_water():
    with open('./dag_water/db_config.json') as file:
        db_config = json.load(file)

    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:5433/{db_config["dbname"]}')
    
    water = pd.read_sql('SELECT * FROM water_table LIMIT 100000', con=engine)
    
    return water.to_json(orient='records')


def transform_water(**kwargs):
    ti = kwargs['ti']
    json_data = json.loads(ti.xcom_pull(task_ids='read_water'))
    water = pd.json_normalize(data=json_data)

    water = transformations_water(water)
    
    logging.info("Datos transformados de agua: %s", water.to_string())  # Limitar la cantidad de datos logueados si es necesario
    
    return water.to_json(orient='records')


def extract_api():
    try:
        client = Socrata("www.datos.gov.co", None)
        results = client.get("tcwu-r53g", limit=2000)
        api_data = pd.DataFrame.from_records(results)
        print(api_data.shape)
        return api_data.to_json(orient='records')
    except Exception as e:
        logging.error("Se produjo un error: %s", e)
    


def transform_api(**kwargs):
    ti = kwargs['ti']
    
    json_data = ti.xcom_pull(task_ids='extract_api') 
    
    api = pd.read_json(json_data, orient='records')
    
    api_transformed = transformations_api(api)
    
    logging.info("Datos transformados de API: %s", api_transformed.to_string())
    
    return api_transformed.to_json(orient='records')





# def merge_task(**kwargs):
#     ti = kwargs['ti']
    
#     spotify_data = json.loads(ti.xcom_pull(task_ids='transform_csv'))
#     grammy_data = json.loads(ti.xcom_pull(task_ids='transform_db'))
    
#     merged_df = merge_datasets(pd.json_normalize(spotify_data), pd.json_normalize(grammy_data))
    
#     logging.info("Fusión de datos completada.")
    
#     return merged_df.to_json(orient='records')



# def load(**kwargs):
#     ti = kwargs["ti"]
#     json_data = json.loads(ti.xcom_pull(task_ids="merge"))
#     data = pd.json_normalize(data=json_data)

#     logging.info("Cargando datos...")
    
#     logging.info("Los datos se han cargado en: tracks")