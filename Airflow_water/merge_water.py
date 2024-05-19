import pandas as pd
import logging

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def merge_datasets(api_done_df, water_cleaned_df):
    try:
        # Verificar y renombrar columnas en api_done_df si es necesario
        if 'municipio' in api_done_df.columns:
            api_done_df = api_done_df.rename(columns={'municipio': 'nombre_municipio'})
            logging.info("Renombrada la columna 'municipio' a 'nombre_municipio' en api_done_df.")
        if 'fecha_terminacion_proyecto' in api_done_df.columns:
            api_done_df = api_done_df.rename(columns={'fecha_terminacion_proyecto': 'fecha_proyecto'})
            logging.info("Renombrada la columna 'fecha_terminacion_proyecto' a 'fecha_proyecto' en api_done_df.")

        # Verificar y renombrar columnas en water_cleaned_df si es necesario
        if 'nombremunicipio' in water_cleaned_df.columns:
            water_cleaned_df = water_cleaned_df.rename(columns={'nombremunicipio': 'nombre_municipio'})
            logging.info("Renombrada la columna 'nombremunicipio' a 'nombre_municipio' en water_cleaned_df.")
        if 'año' not in water_cleaned_df.columns:
            if 'fecha' in water_cleaned_df.columns:
                water_cleaned_df = water_cleaned_df.rename(columns={'fecha': 'año'})
                logging.info("Renombrada la columna 'fecha' a 'año' en water_cleaned_df.")
            else:
                logging.error("La columna de fecha 'año' no existe en water_cleaned_df.")
                raise KeyError("La columna de fecha 'año' no existe en water_cleaned_df.")
        
        logging.info("Revisando los datos antes del filtrado.")
        logging.info("Datos en api_done_df antes del filtrado:\n%s", api_done_df.head().to_string())
        logging.info("Datos en water_cleaned_df antes del filtrado:\n%s", water_cleaned_df.head().to_string())
        
        logging.info("Iniciando el filtrado del dataset de proyectos para incluir solo los años 2018 y 2019.")
        # Filtrar el dataset de proyectos para incluir solo los años 2018 y 2019
        api_done_filtered_df = api_done_df[api_done_df['fecha_proyecto'].dt.year.isin([2018, 2019])]
        logging.info(f"Filtrado del dataset de proyectos completado. Total de filas: {api_done_filtered_df.shape[0]}")
        
        logging.info("Datos en api_done_filtered_df después del filtrado:\n%s", api_done_filtered_df.head().to_string())
        
        logging.info("Iniciando el filtrado del dataset de calidad del agua para incluir solo las fechas de medición a partir de 2018.")
        # Filtrar el dataset de calidad del agua para incluir solo las fechas de medición a partir de 2018
        water_cleaned_filtered_df = water_cleaned_df[water_cleaned_df['año'].dt.year >= 2018]
        logging.info(f"Filtrado del dataset de calidad del agua completado. Total de filas: {water_cleaned_filtered_df.shape[0]}")
        
        logging.info("Datos en water_cleaned_filtered_df después del filtrado:\n%s", water_cleaned_filtered_df.head().to_string())
        
        # Eliminar columnas duplicadas
        api_done_filtered_df = api_done_filtered_df.loc[:, ~api_done_filtered_df.columns.duplicated()]
        water_cleaned_filtered_df = water_cleaned_filtered_df.loc[:, ~water_cleaned_filtered_df.columns.duplicated()]
        
        logging.info("Iniciando el merge de los datasets.")
        # Realizar el merge
        merged_df = pd.merge_asof(
            water_cleaned_filtered_df.sort_values('año'),
            api_done_filtered_df.sort_values('fecha_proyecto'),
            by='nombre_municipio',
            left_on='año',
            right_on='fecha_proyecto',
            direction='backward'
        )
        logging.info("Merge completado.")
        
        logging.info("Eliminando columnas redundantes.")
        # Eliminar columnas redundantes excepto "fecha_de_corte"
        columns_to_drop = ['codigo_departamento', 'departamento', 'c_digo_divipola_municipio', 'num_municipios']
        merged_df = merged_df.drop(columns=columns_to_drop)
        logging.info("Columnas redundantes eliminadas.")
        
        logging.info("Llenando las entradas faltantes con valores predeterminados.")
        # Llenar las entradas faltantes
        merged_df.fillna({
            'indicador': 'Ausencia de proyecto',
            'nombre_proyecto': 'Ausencia de proyecto',
            'origen': 'Ausencia de proyecto',
            'estado_seguimiento': 'Ausencia de proyecto',
            'región': 'Ausencia de proyecto',
            'aporte_nacion': -1,
            'contrapartida': -1,
            'total_financiamiento': -1,
            'fecha_proyecto': -1,
            'duracion_proyecto_dias': -1
        }, inplace=True)
        logging.info("Entradas faltantes llenadas.")
        
        # logging.info("Guardando el DataFrame resultante en un archivo CSV.")
        # # Guardar el DataFrame resultante en un archivo CSV
        # merged_df.to_csv('merged_water.csv', index=False)
        # logging.info("Archivo CSV guardado exitosamente.")
        
        # Resultados
        logging.info(f"Municipios con proyectos: {merged_df[merged_df['nombre_proyecto'] != 'Ausencia de proyecto']['nombre_municipio'].nunique()}")
        logging.info(f"Número total de filas en el DataFrame resultante: {merged_df.shape[0]}")
        logging.info(f"Años disponibles en el DataFrame resultante: {merged_df['año'].dt.year.unique()}")
        
        # Devolver el DataFrame mergeado como JSON
        return merged_df.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error durante el proceso de merge: {e}")
        raise
