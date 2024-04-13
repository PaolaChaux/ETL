import json
import psycopg2
import pandas as pd
    
def extract_data(filename, db_name, table_name):
    try:
        # Cargar configuración de conexión desde un archivo JSON
        with open(filename, 'r') as file:
            config = json.load(file)

        # Conectar a la base de datos
        connection = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            dbname=db_name
        )

        # Consultar todos los datos de la tabla especificada
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)

        # Convertir el DataFrame a JSON
        data_json = df.to_json(orient='records')
        
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
