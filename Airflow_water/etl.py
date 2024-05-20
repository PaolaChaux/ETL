import pandas as pd
import sqlalchemy
import json
import logging
from sqlalchemy import create_engine
import psycopg2
from sodapy import Socrata
import great_expectations as ge
from sklearn.preprocessing import MinMaxScaler
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



def expectation_water(**kwargs):
    ti = kwargs['ti']
    water_json = ti.xcom_pull(task_ids='transform_water')
    df_water = pd.read_json(water_json, orient='records')
    water_ge = ge.from_pandas(df_water)
    
    logging.info("Validating column names")
    expected_columns = [
        'numero_parametros_promedio', 'nombre_parametro_analisis', 'irca_promedio', 
        'nombre_municipio', 'nombre_departamento', 'año', 'is_top_20', 
        'rango_irca', 'tratamiento_categoria', 'proporción_crítica'
    ]
    water_ge.expect_table_columns_to_match_ordered_list(expected_columns)
    
    logging.info("Validating column types")
    water_ge.expect_column_values_to_be_of_type('numero_parametros_promedio', 'int64')
    water_ge.expect_column_values_to_be_of_type('irca_promedio', 'float64')
    water_ge.expect_column_values_to_be_of_type('nombre_municipio', 'str')
    water_ge.expect_column_values_to_be_of_type('nombre_departamento', 'str')
    water_ge.expect_column_values_to_be_of_type('año', 'datetime64[ns]')
    
    logging.info("Validating place name normalization")
    water_ge.expect_column_values_to_match_regex('nombre_departamento', r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$')
    water_ge.expect_column_values_to_match_regex('nombre_municipio', r'^[a-z]+(?: [a-z]+)*$')
    
    logging.info("Validating categorical column values")
    water_ge.expect_column_values_to_be_in_set('rango_irca', [
        'Sin información', 'Sin riesgo', 'Riesgo bajo', 
        'Riesgo medio', 'Riesgo alto', 'Riesgo inviable sanitariamente', 'No clasificado'
    ])
    water_ge.expect_column_values_to_be_in_set('tratamiento_categoria', [
        'Sin tratamiento', 'Tratamiento completo', 'Tratamiento parcial'
    ])
    
    logging.info("Validating scaled values")
    water_ge.expect_column_values_to_be_between('proporción_crítica', 0, 1)
    
    results = water_ge.validate()
    logging.info(f"Validation results: {results}")
    
    if not results['success']:
        logging.error("Water data validation failed")
        raise ValueError("Water data validation failed")
    
    return df_water.to_json(orient='records')




def expectation_api(**kwargs):
    ti = kwargs['ti']
    api_json = ti.xcom_pull(task_ids='transform_api')
    df_api = pd.read_json(api_json, orient='records')
    api_ge = ge.from_pandas(df_api)
    
    logging.info("Validating column names")
    expected_columns = [
        'nombre_municipio', 'fecha_proyecto', 'codigo_departamento', 'num_municipios',
        'departamento', 'región', 'total_financiamiento', 'duracion_proyecto_dias'
    ]
    api_ge.expect_table_columns_to_match_ordered_list(expected_columns)
    
    logging.info("Validating column types")
    api_ge.expect_column_values_to_be_of_type('nombre_municipio', 'str')
    api_ge.expect_column_values_to_be_of_type('fecha_proyecto', 'datetime64[ns]')
    api_ge.expect_column_values_to_be_of_type('codigo_departamento', 'int64')
    api_ge.expect_column_values_to_be_of_type('num_municipios', 'int64')
    api_ge.expect_column_values_to_be_of_type('departamento', 'str')
    api_ge.expect_column_values_to_be_of_type('región', 'str')
    api_ge.expect_column_values_to_be_of_type('total_financiamiento', 'float64')
    api_ge.expect_column_values_to_be_of_type('duracion_proyecto_dias', 'int64')
    
    logging.info("Validating place name normalization")
    api_ge.expect_column_values_to_match_regex('departamento', r'^[A-Z ]+$')
    api_ge.expect_column_values_to_match_regex('nombre_municipio', r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$')
    
    logging.info("Validating numerical ranges")
    api_ge.expect_column_values_to_be_between('total_financiamiento', 0, 1e9)  # Ajusta según los valores esperados
    api_ge.expect_column_values_to_be_between('duracion_proyecto_dias', 0, 1e4)  # Ajusta según los valores esperados
    
    logging.info("Remove parentheses validation")
    api_ge.expect_column_values_to_not_match_regex('nombre_municipio', r"\(.*?\)")
    
    logging.info("Space and capitalize validation")
    api_ge.expect_column_values_to_match_regex('nombre_municipio', r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$')
    
    results = api_ge.validate()
    logging.info(f"Validation results: {results}")
    
    if not results['success']:
        logging.error("API data validation failed")
        raise ValueError("API data validation failed")
    
    return df_api.to_json(orient='records')




def merge_task(**kwargs):
    ti = kwargs['ti']
    
    logging.info("Recuperando los datos transformados de agua y API desde XCom.")
    # Recuperar los datos transformados de agua y API desde XCom
    water_json = ti.xcom_pull(task_ids='transform_water')
    api_json = ti.xcom_pull(task_ids='transform_api')
    
    logging.info("Convirtiendo los datos de JSON a DataFrame.")
    water_cleaned_df = pd.read_json(water_json, orient='records')
    api_done_df = pd.read_json(api_json, orient='records')
    
    logging.info("Ejecutando la función de merge.")
    # Ejecutar la función de merge
    merged_json = merge_datasets(api_done_df, water_cleaned_df)
    
    logging.info("Merge completado y datos convertidos a JSON.")
    return merged_json





  
  
  











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
    
