import pandas as pd
import sqlalchemy
import json
import logging
from sqlalchemy import create_engine
import psycopg2
from sodapy import Socrata
from transform_dag import transformations_api
from transform_dag import transformations_water
from merge_water import merge_datasets
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_water():
    with open('./dag_water/db_config.json') as file:
        db_config = json.load(file)

    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:5433/{db_config["dbname"]}')
    
    water = pd.read_sql('SELECT * FROM water_table LIMIT 100000', con=engine)
    
    return water.to_json(orient='records')


# def read_water():
#     with open('./dag_water/db_config.json') as file:
#         db_config = json.load(file)

#     engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:5433/{db_config["dbname"]}')
    
#     # Usar el nombre correcto de la columna de fecha
#     query = '''
#     SELECT *
#     FROM water_table
#     WHERE EXTRACT(YEAR FROM "Año") >= 2018
#     '''
    
#     water = pd.read_sql(query, con=engine)
    
#     return water.to_json(orient='records')



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


def merge_task(**kwargs):
    ti = kwargs['ti']
    
    logging.info("Recuperando los datos transformados de agua y API desde XCom.")
    # Recuperar los datos transformados de agua y API desde XCom
    water_json = ti.xcom_pull(task_ids='transform_water')
    api_json = ti.xcom_pull(task_ids='transform_api')
    
    logging.info("Convirtiendo los datos de JSON a DataFrame.")
    water_cleaned_df = pd.read_json(water_json, orient='records')
    api_done_df = pd.read_json(api_json, orient='records')
    
    logging.info("Asegurando que las columnas son de tipo string.")
    # Asegurar que las columnas son de tipo string
    water_cleaned_df['nombre_departamento'] = water_cleaned_df['nombre_departamento'].astype(str)
    water_cleaned_df['nombre_municipio'] = water_cleaned_df['nombre_municipio'].astype(str)
    api_done_df['departamento'] = api_done_df['departamento'].astype(str)
    api_done_df['nombre_municipio'] = api_done_df['nombre_municipio'].astype(str)
    
    logging.info("Creando claves únicas para el merge.")
    # Crear una clave única para el merge en ambos datasets
    water_cleaned_df['clave'] = water_cleaned_df['nombre_departamento'].str.lower().str.strip() + "_" + water_cleaned_df['nombre_municipio'].str.lower().str.strip()
    api_done_df['clave'] = api_done_df['departamento'].str.lower().str.strip() + "_" + api_done_df['municipio'].str.lower().str.strip()

    # Loggear algunas claves únicas para verificar
    logging.info(f"Claves únicas en water_cleaned_df:\n{water_cleaned_df['clave'].unique()[:10]}")
    logging.info(f"Claves únicas en api_done_df:\n{api_done_df['clave'].unique()[:10]}")

    logging.info("Realizando el merge de los datasets.")
    # Realizar el merge
    merged_df = pd.merge(water_cleaned_df, api_done_df, on='clave', how='inner')
    
    # Loggear el número de filas y columnas del DataFrame combinado
    num_filas, num_columnas = merged_df.shape
    logging.info(f"El DataFrame combinado tiene {num_filas} filas y {num_columnas} columnas.")
    
    # Loggear las primeras filas del DataFrame combinado
    logging.info(f"Primeras filas del DataFrame combinado:\n{merged_df.head()}")
    
    logging.info("Merge completado y datos convertidos a JSON.")
    return merged_df.to_json(orient='records')








# def merge_task(**kwargs):
#     ti = kwargs['ti']
    
#     logging.info("Recuperando los datos transformados de agua y API desde XCom.")
#     # Recuperar los datos transformados de agua y API desde XCom
#     water_json = ti.xcom_pull(task_ids='transform_water')
#     api_json = ti.xcom_pull(task_ids='transform_api')
    
#     logging.info("Convirtiendo los datos de JSON a DataFrame.")
#     water_cleaned_df = pd.read_json(water_json, orient='records')
#     api_done_df = pd.read_json(api_json, orient='records')
    
    
#     logging.info("Ejecutando la función de merge.")
#     # Ejecutar la función de merge
#     merged_json = merge_datasets(api_done_df, water_cleaned_df)
    
#     logging.info("Merge completado y datos convertidos a JSON.")
#     return merged_json






# def load(**kwargs):
#     ti = kwargs['ti']
#     json_data = json.loads(ti.xcom_pull(task_ids="merge_task"))
#     logging.info(f"Data coming from extract: {json_data}")
    
#     df_water = pd.json_normalize(json_data)
#     df_water.fillna(value=pd.NA, inplace=True)  # Normalizar NaN a NA para compatibilidad con la base de datos

#     try:
#         with open('db_config.json', 'r') as config_json, psycopg2.connect(**json.load(config_json)) as conx:
#             cursor = conx.cursor()
#             # Creación de tablas de dimensiones y tabla de hechos
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS dimension_date (
#                     "ID_Tiempo" SERIAL PRIMARY KEY,
#                     "Año" DATE NOT NULL
#                 );
#             """)
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS dimension_ubication (
#                     "ID_Ubicacion" SERIAL PRIMARY KEY,
#                     "nombre_departamento" VARCHAR(255) NOT NULL,
#                     "div_dpto" INT NOT NULL,
#                     "nombre_municipio" VARCHAR(255) NOT NULL,
#                     "divi_muni" INT NOT NULL
#                 );
#             """)
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS dimension_parameters (
#                     "ID_Parametro" SERIAL PRIMARY KEY,
#                     "nombre_parametro_analisis" VARCHAR(255) NOT NULL
#                 );
#             """)
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS dimension_tratamiento (
#                     "ID_Rango" SERIAL PRIMARY KEY,
#                     "rango_irca" VARCHAR(255) NOT NULL,
#                     "tratamiento_categoría" VARCHAR(255) NOT NULL
#                 );
#             """)
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS Fact_WaterQuality (
#                     "ID_Medicion" SERIAL PRIMARY KEY,
#                     "ID_Tiempo" INT NOT NULL REFERENCES dimension_date("ID_Tiempo"),
#                     "ID_Ubicacion" INT NOT NULL REFERENCES dimension_ubication("ID_Ubicacion"),
#                     "ID_Parametro" INT NOT NULL REFERENCES dimension_parameters("ID_Parametro"),
#                     "ID_Rango" INT NOT NULL REFERENCES dimension_tratamiento("ID_Rango"),
#                     "irca_minimo" FLOAT NOT NULL,
#                     "irca_maximo" FLOAT NOT NULL,
#                     "irca_promedio" FLOAT NOT NULL,
#                     "numero_parametros_promedio" INT NOT NULL,
#                     "porcentaje_muestras_tratadas" INT NOT NULL,
#                     "diferencia_muestras_tratadas_sin_tratar" INT NOT NULL,
#                     "rango_parametros_analizados" INT NOT NULL
#                 );
#             """)

#             for year in df_water['Año'].drop_duplicates():
#                 cursor.execute("""
#                     INSERT INTO dimension_date ("Año") VALUES (%s) ON CONFLICT ("Año") DO NOTHING RETURNING "ID_Tiempo";
#                 """, (year,))
#                 id_tiempo = cursor.fetchone()[0] if cursor.rowcount != 0 else None
            
#             conx.commit()
#             logging.info("Data has been successfully loaded to the database.")

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         if conx:
#             conx.rollback()

#     finally:
#         if cursor:
#             cursor.close()

#     return "Data loaded successfully"
    
