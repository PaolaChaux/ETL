import pandas as pd
import sqlalchemy
import json
import logging
from sqlalchemy import create_engine
from time import sleep
from confluent_kafka import Producer
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
    
    logging.info("Validating column types")
    water_ge.expect_column_values_to_be_of_type('numero_parametros_promedio', 'int64')
    water_ge.expect_column_values_to_be_of_type('irca_promedio', 'float64')
    water_ge.expect_column_values_to_be_of_type('nombre_municipio', 'str')
    water_ge.expect_column_values_to_be_of_type('nombre_departamento', 'str')
    water_ge.expect_column_values_to_be_of_type('año', 'int64')  # Ajustamos a 'int64' porque 'año' es un año extraído como int

    logging.info("Validating place name normalization")
    water_ge.expect_column_values_to_match_regex('nombre_departamento', r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$')
    water_ge.expect_column_values_to_match_regex('nombre_municipio', r'^[A-Z][a-z]+(?: [A-Z][a-z]+)*$')
    
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





  
  
  
def load(**kwargs):
    csv_path = '/root/airflow_water12/dag_water/merged_data.csv'
    db_config_path = '/root/airflow_water12/dag_water/db_config.json'

    logging.info("Leyendo datos")

    df_water = pd.read_csv(csv_path)
    df_water.fillna(value=pd.NA, inplace=True)  # Normalizar NaN a NA para compatibilidad con la base de datos
    df_water['fecha_proyecto'] = df_water['fecha_proyecto'].apply(lambda x: '1970-01-01' if x == -1.0 else pd.to_datetime(x, errors='coerce').date())

    try:
        with open(db_config_path, 'r') as config_json:
            db_config = json.load(config_json)
            conx = psycopg2.connect(**db_config)
            cursor = conx.cursor()

            # Mostrar la base de datos y la ubicación
            logging.info(f"Conectado a la base de datos: {db_config['dbname']} en {db_config['host']}")

            cursor.execute("DROP TABLE IF EXISTS Fact_WaterQuality CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS dimension_proyecto CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS dimension_tratamiento CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS dimension_parameters CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS dimension_ubication CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS dimension_date CASCADE;")

            # Creación de tablas de dimensiones y tabla de hechos con restricciones únicas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dimension_date (
                    "ID_Tiempo" SERIAL PRIMARY KEY,
                    "Año" INT NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dimension_ubication (
                    "ID_Ubicacion" SERIAL PRIMARY KEY,
                    "nombre_departamento" VARCHAR(255) NOT NULL,
                    "div_dpto" INT NOT NULL,
                    "nombre_municipio_x" VARCHAR(255) NOT NULL,
                    "divi_muni" INT NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dimension_parameters (
                    "ID_Parametro" SERIAL PRIMARY KEY,
                    "nombre_parametro_analisis" VARCHAR(255) NOT NULL,
                    "numero_parametros_promedio" INT NOT NULL,
                    "is_top_20" BOOLEAN NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dimension_tratamiento (
                    "ID_Rango" SERIAL PRIMARY KEY,
                    "rango_irca" VARCHAR(255) NOT NULL,
                    "tratamiento_categoría" VARCHAR(255) NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dimension_proyecto (
                    "ID_Proyecto" SERIAL PRIMARY KEY,
                    "indicador" VARCHAR(255) NOT NULL,
                    "nombre_proyecto" VARCHAR(255) NOT NULL,
                    "origen" VARCHAR(255) NOT NULL,
                    "estado_seguimiento" VARCHAR(255) NOT NULL,
                    "num_municipios" INT NOT NULL,
                    "region" VARCHAR(255) NOT NULL,
                    "total_financiamiento" FLOAT NOT NULL,
                    "duracion_proyecto_dias" INT NOT NULL,
                    "ano_proyecto" INT NOT NULL,
                    "fecha_proyecto" DATE NOT NULL,
                    "clave" VARCHAR(255) NOT NULL,
                    "codigo_departamento" VARCHAR(255) NOT NULL,
                    "c_digo_divipola_municipio" VARCHAR(255) NOT NULL,
                    "nombre_municipio_y" VARCHAR(255) NOT NULL
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Fact_WaterQuality (
                    "ID_Medicion" SERIAL PRIMARY KEY,
                    "ID_Tiempo" INT NOT NULL REFERENCES dimension_date("ID_Tiempo"),
                    "ID_Ubicacion" INT NOT NULL REFERENCES dimension_ubication("ID_Ubicacion"),
                    "ID_Parametro" INT NOT NULL REFERENCES dimension_parameters("ID_Parametro"),
                    "ID_Rango" INT NOT NULL REFERENCES dimension_tratamiento("ID_Rango"),
                    "ID_Proyecto" INT NOT NULL REFERENCES dimension_proyecto("ID_Proyecto"),
                    "irca_promedio" FLOAT NOT NULL,
                    "proporcion_critica" FLOAT NOT NULL
                );
            """)
            conx.commit()
            logging.info("Tablas creadas exitosamente.")

            for year in df_water['año']:
                cursor.execute("""
                    INSERT INTO dimension_date ("Año") VALUES (%s) RETURNING "ID_Tiempo";
                """, (year,))
                id_tiempo = cursor.fetchone()[0]
                logging.info(f"Insertado año {year} con ID {id_tiempo}")

            for _, row in df_water.iterrows():
                cursor.execute("""
                    INSERT INTO dimension_ubication ("nombre_departamento", "div_dpto", "nombre_municipio_x", "divi_muni")
                    VALUES (%s, %s, %s, %s) RETURNING "ID_Ubicacion";
                """, (row['nombre_departamento'], row['Div_dpto'], row['nombre_municipio_x'], row['Divi_muni']))
                id_ubicacion = cursor.fetchone()[0]
                logging.info(f"Insertado ubicación con ID {id_ubicacion}")

                cursor.execute("""
                    INSERT INTO dimension_parameters ("nombre_parametro_analisis", "numero_parametros_promedio", "is_top_20")
                    VALUES (%s, %s, %s) RETURNING "ID_Parametro";
                """, (row['nombre_parametro_analisis'], row['numero_parametros_promedio'], row['is_top_20']))
                id_parametro = cursor.fetchone()[0]
                logging.info(f"Insertado parámetro con ID {id_parametro}")

                cursor.execute("""
                    INSERT INTO dimension_tratamiento ("rango_irca", "tratamiento_categoría")
                    VALUES (%s, %s) RETURNING "ID_Rango";
                """, (row['rango_irca'], row['tratamiento_categoria']))
                id_rango = cursor.fetchone()[0]
                logging.info(f"Insertado tratamiento con ID {id_rango}")

                cursor.execute("""
                    INSERT INTO dimension_proyecto ("indicador", "nombre_proyecto", "origen", "estado_seguimiento", "num_municipios", "region", "total_financiamiento", "duracion_proyecto_dias", "ano_proyecto", "fecha_proyecto", "clave", "codigo_departamento", "c_digo_divipola_municipio", "nombre_municipio_y")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING "ID_Proyecto";
                """, (row['indicador'], row['nombre_proyecto'], row['origen'], row['estado_seguimiento'], row['num_municipios'], row['región'], row['total_financiamiento'], row['duracion_proyecto_dias'], row['año_proyecto'], row['fecha_proyecto'], row['clave'], row['codigo_departamento'], row['c_digo_divipola_municipio'], row['nombre_municipio_y']))
                id_proyecto = cursor.fetchone()[0]
                logging.info(f"Insertado proyecto con ID {id_proyecto}")

                cursor.execute("""
                    INSERT INTO Fact_WaterQuality ("ID_Tiempo", "ID_Ubicacion", "ID_Parametro", "ID_Rango", "ID_Proyecto", "irca_promedio", "proporcion_critica")
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (id_tiempo, id_ubicacion, id_parametro, id_rango, id_proyecto, row['irca_promedio'], row['proporción_crítica']))
                logging.info(f"Insertado medición con ID_Tiempo {id_tiempo}, ID_Ubicacion {id_ubicacion}, ID_Parametro {id_parametro}, ID_Rango {id_rango}, ID_Proyecto {id_proyecto}")

            conx.commit()
            logging.info("Los datos se han cargado exitosamente a la base de datos.")
            logging.info(f"Primeras filas del DataFrame cargado:\n{df_water.head()}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        if 'conx' in locals() and conx:
            conx.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conx' in locals() and conx:
            conx.close()

    return "Data loaded successfully"






def consultar_datos(filename, db_name, table_name):
    try:
        with open(filename, 'r') as file:
            config = json.load(file)

        connection = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            dbname=db_name,
            port=config.get("port", 3033)
        )

        query = f"SELECT * FROM {table_name} LIMIT 100"
        data = pd.read_sql(query, connection)

        return data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al consultar datos: {error}")
        return None
    finally:
        if 'connection' in locals():
            connection.close()

def stream_data():
    filename = '/root/airflow_water12/dag_water/db_config.json'
    db_name = 'water'
    table_name = 'dimension_tratamiento'
    df = consultar_datos(filename, db_name, table_name)
    kafka_bootstrap_servers = ['localhost:9092']

    producer = Producer({
        'bootstrap.servers': ','.join(kafka_bootstrap_servers)
    })

    for i in range(len(df)):
        row_json = df.iloc[i].to_json()
        producer.produce("kafka-water", value=row_json)
        print(f"new message sent: {row_json}")
        sleep(1)

    producer.flush()

    print("Fin del envio")











    
