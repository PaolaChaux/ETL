import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def merge_datasets(api_done_df, water_cleaned_df):
    logging.info("Asegurando que las columnas son de tipo string.")
    # Asegurar que las columnas son de tipo string
    water_cleaned_df['nombre_departamento'] = water_cleaned_df['nombre_departamento'].astype(str)
    water_cleaned_df['nombre_municipio'] = water_cleaned_df['nombre_municipio'].astype(str)
    api_done_df['departamento'] = api_done_df['departamento'].astype(str)
    api_done_df['municipio'] = api_done_df['municipio'].astype(str)
    
    logging.info("Creando claves únicas para el merge.")
    # Crear una clave única para el merge en ambos datasets
    water_cleaned_df['clave'] = water_cleaned_df['nombre_departamento'].str.lower().str.strip() + "_" + water_cleaned_df['nombre_municipio'].str.lower().str.strip()
    api_done_df['clave'] = api_done_df['departamento'].str.lower().str.strip() + "_" + api_done_df['nombre_municipio'].str.lower().str.strip()

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

