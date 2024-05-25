import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevelname)s - %(message)s')

def merge_datasets(api_done_df, water_cleaned_df):
    logging.info("Verificando las columnas disponibles en los DataFrames.")
    logging.info(f"Columnas en water_cleaned_df: {water_cleaned_df.columns.tolist()}")
    logging.info(f"Columnas en api_done_df: {api_done_df.columns.tolist()}")
    

    if 'nombre_departamento' not in water_cleaned_df.columns or 'nombre_municipio' not in water_cleaned_df.columns or 'año' not in water_cleaned_df.columns:
        raise KeyError("Las columnas 'nombre_departamento', 'nombre_municipio' y 'año' deben estar presentes en water_cleaned_df.")
    if 'departamento' not in api_done_df.columns or 'nombre_municipio' not in api_done_df.columns or 'año_proyecto' not in api_done_df.columns:
        raise KeyError("Las columnas 'departamento', 'nombre_municipio' y 'año_proyecto' deben estar presentes en api_done_df.")

    
    logging.info("Filtrando los DataFrames para incluir solo los años 2017, 2018 y 2019.")

    water_cleaned_df = water_cleaned_df[water_cleaned_df['año'].isin([2017, 2018, 2019])]
    api_done_df = api_done_df[api_done_df['año_proyecto'].isin([2017, 2018, 2019])]

    logging.info("Creando claves únicas para el merge.")

    water_cleaned_df['clave'] = (water_cleaned_df['nombre_departamento'].str.lower().str.strip() + "_" + 
                                 water_cleaned_df['nombre_municipio'].str.lower().str.strip() + "_" + 
                                 water_cleaned_df['año'].astype(str))
    api_done_df['clave'] = (api_done_df['departamento'].str.lower().str.strip() + "_" + 
                            api_done_df['nombre_municipio'].str.lower().str.strip() + "_" + 
                            api_done_df['año_proyecto'].astype(str))

   
    logging.info(f"Claves únicas en water_cleaned_df:\n{water_cleaned_df['clave'].unique()[:10]}")
    logging.info(f"Claves únicas en api_done_df:\n{api_done_df['clave'].unique()[:10]}")

    logging.info("Realizando el merge de los datasets.")

    merged_df = pd.merge(water_cleaned_df, api_done_df, on='clave', how='left')
    
    logging.info("Llenando valores faltantes con valores predeterminados.")

    for column in merged_df.columns:
        if merged_df[column].dtype == 'object':
            merged_df[column].fillna('Ausencia de proyecto', inplace=True)
        elif merged_df[column].dtype in ['int64', 'float64']:
            merged_df[column].fillna(-1, inplace=True)
    
 
    num_filas, num_columnas = merged_df.shape
    logging.info(f"El DataFrame combinado tiene {num_filas} filas y {num_columnas} columnas.")
    

    logging.info(f"Primeras filas del DataFrame combinado:\n{merged_df.head()}")

  
    num_proyectos = merged_df['nombre_proyecto'].nunique()
    anos_presentes = merged_df['año'].unique()
    logging.info(f"Número de proyectos únicos: {num_proyectos}")
    logging.info(f"Años presentes en los datos combinados: {anos_presentes}")


    output_dir = '/root/airflow_water/dag_water/'
    os.makedirs(output_dir, exist_ok=True) 
    csv_path = os.path.join(output_dir, 'merged_data.csv')
    merged_df.to_csv(csv_path, index=False)
    logging.info(f"Archivo CSV guardado en: {csv_path}")

    logging.info("Merge completado y datos convertidos a JSON.")
    return merged_df.to_json(orient='records')


