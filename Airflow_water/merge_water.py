import pandas as pd
import logging

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def merge_datasets(api_done_df, water_cleaned_df):
    try:
        logging.info("Iniciando el filtrado del dataset de proyectos para incluir solo los años 2018 y 2019.")
        # Filtrar el dataset de proyectos para incluir solo los años 2018 y 2019
        api_done_filtered_df = api_done_df[api_done_df['fecha_proyecto'].dt.year.isin([2018, 2019])]
        logging.info(f"Filtrado del dataset de proyectos completado. Total de filas: {api_done_filtered_df.shape[0]}")
        
        logging.info("Iniciando el filtrado del dataset de calidad del agua para incluir solo las fechas de medición a partir de 2018.")
        # Filtrar el dataset de calidad del agua para incluir solo las fechas de medición a partir de 2018
        water_cleaned_filtered_df = water_cleaned_df[water_cleaned_df['año'].dt.year >= 2018]
        logging.info(f"Filtrado del dataset de calidad del agua completado. Total de filas: {water_cleaned_filtered_df.shape[0]}")
        
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
        columns_to_drop = ['c_digo_divipola_departamento', 'departamento', 'c_digo_divipola_municipio']
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
        
        logging.info("Guardando el DataFrame resultante en un archivo CSV.")
        # Guardar el DataFrame resultante en un archivo CSV
        merged_df.to_csv('merged_water.csv', index=False)
        logging.info("Archivo CSV guardado exitosamente.")
        
        # Resultados
        logging.info(f"Municipios con proyectos: {merged_df[merged_df['nombre_proyecto'] != 'Ausencia de proyecto']['nombre_municipio'].nunique()}")
        logging.info(f"Número total de filas en el DataFrame resultante: {merged_df.shape[0]}")
        logging.info(f"Años disponibles en el DataFrame resultante: {merged_df['año'].dt.year.unique()}")
        
        # Devolver el DataFrame mergeado como JSON
        return merged_df.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error durante el proceso de merge: {e}")
        raise
