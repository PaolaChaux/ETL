import pandas as pd
import logging
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevelname)s - %(message)s')

def merge_datasets(api_done_df, water_cleaned_df):
    logging.info("Verificando las columnas disponibles en los DataFrames.")
    logging.info(f"Columnas en water_cleaned_df: {water_cleaned_df.columns.tolist()}")
    logging.info(f"Columnas en api_done_df: {api_done_df.columns.tolist()}")
    
    # Asegurar que las columnas necesarias están presentes
    if 'nombre_departamento' not in water_cleaned_df.columns or 'nombre_municipio' not in water_cleaned_df.columns or 'año' not in water_cleaned_df.columns:
        raise KeyError("Las columnas 'nombre_departamento', 'nombre_municipio' y 'año' deben estar presentes en water_cleaned_df.")
    if 'departamento' not in api_done_df.columns or 'nombre_municipio' not in api_done_df.columns or 'año_proyecto' not in api_done_df.columns:
        raise KeyError("Las columnas 'departamento', 'nombre_municipio' y 'año_proyecto' deben estar presentes en api_done_df.")

    logging.info("Asegurando que las columnas son de tipo string y año como int.")
    # Asegurar que las columnas son de tipo string y año como int
    water_cleaned_df['nombre_departamento'] = water_cleaned_df['nombre_departamento'].astype(str)
    water_cleaned_df['nombre_municipio'] = water_cleaned_df['nombre_municipio'].astype(str)
    api_done_df['departamento'] = api_done_df['departamento'].astype(str)
    api_done_df['nombre_municipio'] = api_done_df['nombre_municipio'].astype(str)
    water_cleaned_df['año'] = water_cleaned_df['año'].astype(int)
    api_done_df['año_proyecto'] = api_done_df['año_proyecto'].astype(int)
    
    logging.info("Filtrando los DataFrames para incluir solo los años 2017, 2018 y 2019.")
    # Filtrar los DataFrames para incluir solo los años 2017, 2018 y 2019
    water_cleaned_df = water_cleaned_df[water_cleaned_df['año'].isin([2017, 2018, 2019])]
    api_done_df = api_done_df[api_done_df['año_proyecto'].isin([2017, 2018, 2019])]

    logging.info("Creando claves únicas para el merge.")
    # Crear una clave única para el merge en ambos datasets
    water_cleaned_df['clave'] = (water_cleaned_df['nombre_departamento'].str.lower().str.strip() + "_" + 
                                 water_cleaned_df['nombre_municipio'].str.lower().str.strip() + "_" + 
                                 water_cleaned_df['año'].astype(str))
    api_done_df['clave'] = (api_done_df['departamento'].str.lower().str.strip() + "_" + 
                            api_done_df['nombre_municipio'].str.lower().str.strip() + "_" + 
                            api_done_df['año_proyecto'].astype(str))

    # Loggear algunas claves únicas para verificar
    logging.info(f"Claves únicas en water_cleaned_df:\n{water_cleaned_df['clave'].unique()[:10]}")
    logging.info(f"Claves únicas en api_done_df:\n{api_done_df['clave'].unique()[:10]}")

    logging.info("Realizando el merge de los datasets.")
    # Realizar el merge con un join izquierdo para incluir todas las filas del DataFrame principal
    merged_df = pd.merge(water_cleaned_df, api_done_df, on='clave', how='left')
    
    logging.info("Llenando valores faltantes con valores predeterminados.")
    # Llenar valores faltantes con valores predeterminados
    for column in merged_df.columns:
        if merged_df[column].dtype == 'object':
            merged_df[column].fillna('Ausencia de proyecto', inplace=True)
        elif merged_df[column].dtype in ['int64', 'float64']:
            merged_df[column].fillna(-1, inplace=True)
    
    # Loggear el número de filas y columnas del DataFrame combinado
    num_filas, num_columnas = merged_df.shape
    logging.info(f"El DataFrame combinado tiene {num_filas} filas y {num_columnas} columnas.")
    
    # Loggear las primeras filas del DataFrame combinado
    logging.info(f"Primeras filas del DataFrame combinado:\n{merged_df.head()}")

    # Loggear el número de proyectos (entradas) y los años presentes
    num_proyectos = merged_df['nombre_proyecto'].nunique()
    anos_presentes = merged_df['año'].unique()
    logging.info(f"Número de proyectos únicos: {num_proyectos}")
    logging.info(f"Años presentes en los datos combinados: {anos_presentes}")

    # Guardar el DataFrame combinado en un archivo CSV
    output_dir = '/root/airflow_water/dag_water/'
    os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe
    csv_path = os.path.join(output_dir, 'merged_data.csv')
    merged_df.to_csv(csv_path, index=False)
    logging.info(f"Archivo CSV guardado en: {csv_path}")

    logging.info("Merge completado y datos convertidos a JSON.")
    return merged_df.to_json(orient='records')


