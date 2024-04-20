import json
import psycopg2
import pandas as pd
from datetime import datetime, timezone

#extraccion de la data para transformacion a esquema estrella
def extract_data(filename, db_name, table_name):
    try:
        with open(filename, 'r') as file:
            config = json.load(file)

        connection = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            dbname=db_name
        )

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)

        df['ano'] = pd.to_datetime(df['ano']).dt.date
        data_json = df.to_json(orient='records', date_format='iso')

        return data_json

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al consultar datos: {error}")
        return None

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    filename = 'db_config.json'
    db_name = 'db_water'
    table_name = 'tabla_data_cleaned'
    datos_json = extract_data(filename, db_name, table_name)

    if datos_json is not None:
        print(datos_json)

#Creacion de esquema de estrella

def create_database(config_file, db_name):
    connection = None
    cursor = None
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)

        connection = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"]
        )
        connection.autocommit = True
        cursor = connection.cursor()

        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}';")
        if cursor.fetchone():
            print(f"La base de datos {db_name} ya existe.")
        else:
            cursor.execute(f"CREATE DATABASE {db_name};")
            print(f"Base de datos {db_name} creada exitosamente!")

    except psycopg2.Error as e:
        print(f"No se puede crear la base de datos: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    config_file_path = 'db_config.json'
    database_name = 'db_star_schema'
    create_database(config_file_path, database_name)



def create_star_schema(config_filename, db_name):
    connection = None
    try:
        with open(config_filename, 'r') as file:
            config = json.load(file)

        config['database'] = db_name
        
        connection = psycopg2.connect(**config)
        cursor = connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_date (
                "ID_Tiempo" SERIAL PRIMARY KEY,
                "Año" DATE NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_ubication (
                "ID_Ubicacion" SERIAL PRIMARY KEY,
                "nombre_departamento" VARCHAR(255) NOT NULL,
                "div_dpto" INT NOT NULL,
                "nombre_municipio" VARCHAR(255) NOT NULL,
                "divi_muni" INT NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_parameters (
                "ID_Parametro" SERIAL PRIMARY KEY,
                "nombre_parametro_analisis" VARCHAR(255) NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_tratamiento (
                "ID_Rango" SERIAL PRIMARY KEY,
                "rango_irca" VARCHAR(255) NOT NULL
                "tratamiento_categoría" VARCHAR(255) NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Fact_WaterQuality (
                "ID_Medicion" SERIAL PRIMARY KEY,
                "ID_Tiempo" INT NOT NULL REFERENCES dimension_date("ID_Tiempo"),
                "ID_Ubicacion" INT NOT NULL REFERENCES dimension_ubication("ID_Ubicacion"),
                "ID_Parametro" INT NOT NULL REFERENCES dimension_parameters("ID_Parametro"),
                "ID_Rango" INT NOT NULL REFERENCES dimension_rango("ID_Rango"),
                "irca_minimo" FLOAT NOT NULL,
                "irca_maximo" FLOAT NOT NULL,
                "irca_promedio" FLOAT NOT NULL,
                "numero_parametros_promedio" INT NOT NULL,
                "porcentaje_muestras_tratadas" INT NOT NULL,
                "diferencia_muestras_tratadas_sin_tratar" INT NOT NULL,
                "rango_parametros_analizados" INT NOT NULL
                
                
            );
        """)
        print("Esquema estrella creado con éxito.")
        connection.commit()
    except psycopg2.Error as e:
        print(f"No se puede crear el esquema estrella: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    config_filename = 'db_config.json'
    db_name = 'db_star_schema'
    create_star_schema(config_filename, db_name)


#INSERTAR DATA EN BD

def insertar_dimension_tiempo(cursor, fecha):
    cursor.execute("""
        INSERT INTO dimension_date ("Año") VALUES (%s) ON CONFLICT DO NOTHING RETURNING "ID_Tiempo";
    """, (fecha,))
    id_tiempo = cursor.fetchone()
    return id_tiempo[0] if id_tiempo else None

def insertar_dimension_ubicacion(cursor, nombre_departamento, div_dpto, nombre_municipio, divi_muni):
    cursor.execute("""
        INSERT INTO dimension_ubication ("nombre_departamento", "div_dpto", "nombre_municipio", "divi_muni")
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING "ID_Ubicacion";
    """, (nombre_departamento, div_dpto, nombre_municipio, divi_muni))
    id_ubicacion = cursor.fetchone()
    return id_ubicacion[0] if id_ubicacion else None

def insertar_dimension_parametros(cursor, nombre_parametro_analisis2):
    cursor.execute("""
        INSERT INTO dimension_parameters ("nombre_parametro_analisis2")
        VALUES (%s) ON CONFLICT DO NOTHING RETURNING "ID_Parametro";
    """, (nombre_parametro_analisis2,))
    id_parametro = cursor.fetchone()
    return id_parametro[0] if id_parametro else None

def insertar_dimension_rango(cursor, rango_irca):
    cursor.execute("""
        INSERT INTO dimension_rango ("rango_irca")
        VALUES (%s) ON CONFLICT DO NOTHING RETURNING "ID_Rango";
    """, (rango_irca,))
    id_rango = cursor.fetchone()
    return id_rango[0] if id_rango else None

def insertar_hechos(cursor, id_tiempo, id_ubicacion, id_parametro, id_rango, irca_minimo, irca_maximo, irca_promedio, numero_parametros_promedio, porcentaje_muestras_tratadas, diferencia_muestras_tratadas_sin_tratar, rango_parametros_analizados):
    cursor.execute("""
        INSERT INTO hchos_irca_mediciones ("ID_Tiempo", "ID_Ubicacion", "ID_Parametro", "ID_Rango", "irca_minimo", "irca_maximo", "irca_promedio", "numero_parametros_promedio", "porcentaje_muestras_tratadas", "diferencia_muestras_tratadas_sin_tratar", "rango_parametros_analizados")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (id_tiempo, id_ubicacion, id_parametro, id_rango, irca_minimo, irca_maximo, irca_promedio, numero_parametros_promedio, porcentaje_muestras_tratadas, diferencia_muestras_tratadas_sin_tratar, rango_parametros_analizados))

def insertar_datos_star_schema(datos_json, config_filename, db_name):
    with open(config_filename, 'r') as file:
        config = json.load(file)
    config['database'] = db_name
    connection = psycopg2.connect(**config)
    cursor = connection.cursor()
    
    try:
        data = json.loads(datos_json)

        for item in data:
            fecha = datetime.fromisoformat(item['ano'].split('T')[0]) 

            id_tiempo = insertar_dimension_tiempo(cursor, fecha)
            id_ubicacion = insertar_dimension_ubicacion(cursor, item['nombre_departamento'], item['div_dpto'], item['nombre_municipio'], item['divi_muni'])
            id_parametro = insertar_dimension_parametros(cursor, item['nombre_parametro_analisis2'])
            id_rango = insertar_dimension_rango(cursor, item['rango_irca'])
            
            insertar_hechos(cursor, id_tiempo, id_ubicacion, id_parametro, id_rango, item['irca_minimo'], item['irca_maximo'], item['irca_promedio'], item['numero_parametros_promedio'], item['porcentaje_muestras_tratadas'], item['diferencia_muestras_tratadas_sin_tratar'], item['rango_parametros_analizados'])
        
        connection.commit()
        print("Todos los datos se insertaron exitosamente.")
    except psycopg2.Error as e:
        print(f"Error al insertar los datos: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    config_filename = 'db_config.json'
    db_name = 'db_star_schema'
    datos_json = datos_json
    insertar_datos_star_schema(datos_json, config_filename, db_name)
