import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import logging
import re

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def dates_water(water):
    water['Año'] = pd.to_datetime(water['Año'])
    return water

def standardize_place_names(water):
    water['NombreDepartamento'] = water['NombreDepartamento'].str.title().str.strip()
    water['NombreMunicipio'] = water['NombreMunicipio'].str.title().str.strip()
    return water

def normalize_text_columns_water(water):
    str_cols = water.select_dtypes(include=['object']).columns
    water[str_cols] = water[str_cols].apply(lambda x: x.str.lower().str.strip())
    return water

def renombrar_columnas_water(water):
    water = water.rename(columns={
        'numeroparametrospromedio': 'numero_parametros_promedio',
        'nombreparametroanalisis2': 'nombre_parametro_analisis',
        'ircapromedio': 'irca_promedio',
        'nombremunicipio': 'nombre_municipio',
        'nombredepartamento': 'nombre_departamento',
        'fecha_terminacion_proyecto': 'fecha_proyecto'
    })
    return water

def scale_columns(water):
    scaler = MinMaxScaler()
    columns_to_scale = ['MuestrasEvaluadas', 'MuestrasTratadas', 'MuestrasSinTratar']
    water[columns_to_scale] = scaler.fit_transform(water[columns_to_scale])
    return water

def filter_top_parameters(water):
    parametros_influencia = water.groupby('NombreParametroAnalisis2')['IrcaPromedio'].mean().sort_values(ascending=False)
    top_20_parametros = parametros_influencia.head(20)
    water['is_top_20'] = water['NombreParametroAnalisis2'].isin(top_20_parametros.index)
    return water

def classify_irca(water):
    def clasificar_irca(irca):
        try:
            if isinstance(irca, str):
                irca = float(irca.replace(',', '.'))
            if irca == 0:
                return 'Sin información'
            elif irca < 5:
                return 'Sin riesgo'
            elif irca < 14:
                return 'Riesgo bajo'
            elif irca < 35:
                return 'Riesgo medio'
            elif irca < 80:
                return 'Riesgo alto'
            elif irca <= 100:
                return 'Riesgo inviable sanitariamente'
            else:
                return 'No clasificado'
        except ValueError:
            return 'No clasificado'
    water.loc[:, 'rango_irca'] = water['IrcaPromedio'].apply(clasificar_irca)
    return water

def categorize_treatment(water):
    """Categorizar el tratamiento de muestras."""
    def categorize(row):
        if row['MuestrasTratadas'] == 0:
            return 'Sin tratamiento'
        elif row['MuestrasTratadas'] == row['MuestrasEvaluadas']:
            return 'Tratamiento completo'
        else:
            return 'Tratamiento parcial'
    water.loc[:, 'tratamiento_categoría'] = water.apply(categorize, axis=1)
    return water

def calculate_critical_proportion(water, threshold=50):
    """Calcular la proporción crítica de IRCA que supera un umbral especificado."""
    def proportion(row):
        if row['IrcaMaximo'] == row['IrcaMinimo']:
            return 0
        else:
            lower_bound = max(threshold, row['IrcaMinimo'])
            if lower_bound > row['IrcaMaximo']:
                return 0
            return (row['IrcaMaximo'] - lower_bound) / (row['IrcaMaximo'] - row['IrcaMinimo'])
    water['Proporción Crítica'] = water.apply(proportion, axis=1)
    return water

def drop_unnecessary_columns_water(water):
    """Eliminar columnas que no son necesarias para el análisis."""
    columns_to_drop = ['MuestrasTratadas', 'MuestrasEvaluadas', 'MuestrasSinTratar',
                       'NumeroParametrosMinimo', 'NumeroParametrosMaximo', 'ResultadoMinimo', 'ResultadoMaximo', 'ResultadoPromedio']
    return water.drop(columns=columns_to_drop)

def standardize_column_names(water):
    water.columns = water.columns.str.lower().str.replace(' ', '_')
    logging.info("Column names standardized")
    return water

def transformations_water(water):
    logging.info("Starting transformations on water data.")
     
  
    
    
    water = normalize_text_columns_water(water)
    logging.info("normalize text colums water succesfully")
    
    water = scale_columns(water)
    logging.info("Scaled numerical columns.")
    
    water = filter_top_parameters(water)
    logging.info("Filtered top influential parameters.")
    
    water = classify_irca(water)
    logging.info("Classified IRCA values into categories.")
    
    water = categorize_treatment(water)
    logging.info("Categorized treatment data.")
    
    water = calculate_critical_proportion(water)
    logging.info("Critical Proportion.")
    
    water = drop_unnecessary_columns_water(water)
    logging.info("Dropped unnecessary columns.")
    
    water = standardize_place_names(water)
    logging.info("Standardized place names.")
    
    water = dates_water(water)
    logging.info("Dates converted successfully.")
    
    water = renombrar_columnas_water(water)
    logging.info("renombrar columnass water succesfully")
    
    logging.info("All transformations applied successfully.")
    return water

        
    






# tranformaciones API:


def remove_parentheses(api):
    api['municipio'] = api['municipio'].str.replace(r"\(.*?\)", "", regex=True)
    return api

def separate_municipalities(api):
    api = api.assign(municipio=api['municipio'].str.split(',')).explode('municipio')
    return api

def space_capitalize(api):
    api['municipio'] = api['municipio'].str.strip().str.capitalize()
    return api


def renombrar_columnas(api):
    api = api.rename(columns={'municipio': 'nombre_municipio', 'fecha_terminacion_proyecto': 'fecha_proyecto', 'c_digo_divipola_departamento': 'codigo_departamento'})
    return api


def dates_api(api):
    api['fecha_proyecto'] = pd.to_datetime(api['fecha_proyecto'])
    api['fecha_de_corte'] = pd.to_datetime(api['fecha_de_corte'])
    return api

def normalize_text_columns(api):
    str_cols = api.select_dtypes(include=['object']).columns
    api[str_cols] = api[str_cols].apply(lambda x: x.str.lower().str.strip())
    return api


def standardize_place_names_api(api):
    api['nombre_municipio'] = api['nombre_municipio'].str.title().str.strip()
    return api



def compute_num_municipios(api):
    api['num_municipios'] = api['c_digo_divipola_municipio'].apply(lambda x: len(x.split(',')))
    return api

def map_regions(api):
    region_mapping = {
        'CHOCO': 'Región Pacífica', 'CAUCA': 'Región Pacífica', 'NARIÑO': 'Región Pacífica', 'VALLE DEL CAUCA': 'Región Pacífica',
        'ARAUCA': 'Región Orinoquía', 'CASANARE': 'Región Orinoquía', 'GUAINÍA': 'Región Orinoquía', 'GUAVIARE': 'Región Orinoquía', 'META': 'Región Orinoquía', 'VICHADA': 'Región Orinoquía',
        'AMAZONAS': 'Región Amazonía', 'CAQUETA': 'Región Amazonía', 'PUTUMAYO': 'Región Amazonía', 'VAUPES': 'Región Amazonía',
        'ANTIOQUIA': 'Región Andina', 'BOYACA': 'Región Andina', 'CALDAS': 'Región Andina', 'CUNDINAMARCA': 'Región Andina', 'HUILA': 'Región Andina', 'NORTE DE SANTANDER': 'Región Andina', 'QUINDIO': 'Región Andina', 'RISARALDA': 'Región Andina', 'SANTANDER': 'Región Andina', 'TOLIMA': 'Región Andina',
        'SAN ANDRES Y PROVIDENCIA': 'Región Insular', 'ATLANTICO': 'Región Caribe', 'BOLIVAR': 'Región Caribe', 'CESAR': 'Región Caribe', 'CORDOBA': 'Región Caribe', 'LA GUAJIRA': 'Región Caribe', 'MAGDALENA': 'Región Caribe', 'SUCRE': 'Región Caribe'
    }
    api['departamento'] = api['departamento'].replace({
        'SAN ANDRES': 'SAN ANDRES Y PROVIDENCIA',
        'N DE SANTANDER': 'NORTE DE SANTANDER'
    }).str.upper()
    api['región'] = api['departamento'].map(region_mapping).fillna('Región Desconocida')
    return api

def calculate_financing(api):
    api['total_financiamiento'] = api['aporte_nacion'] + api['contrapartida']
    return api

def calculate_project_duration(api):
    api['duracion_proyecto_dias'] = (api['fecha_de_corte'] - api['fecha_proyecto']).dt.days
    return api


def drop_unnecessary_columns(api):
    api.drop(['fecha_de_corte', 'contrapartida', 'aporte_nacion'], axis=1, inplace=True)
    return api


def transformations_api(api):
    logging.info("Starting transformations on API data.")
    
    api = remove_parentheses(api)
    logging.info("Elimination of parentheses within municipalities successfully.")
    
    api = separate_municipalities(api)
    logging.info("Separates records with multiple municipalities into individual rows successfully.")
    
    api = space_capitalize(api)
    logging.info("Elimination of extra spaces and capitalization of each municipality name successful.")
    
    api = normalize_text_columns(api)
    logging.info("Text columns normalized successfully.")
    
    api = standardize_place_names_api(api)
    logging.info(" Standardized place names API successful.")
   
    api = compute_num_municipios(api)
    logging.info("Number of municipalities computed successfully.")
    
    api = map_regions(api)
    logging.info("Regions mapped successfully.")
    
    api = calculate_financing(api)
    logging.info("Project financing calculated successfully.")
    
    api = calculate_project_duration(api)
    logging.info("Project duration calculated successfully.")
    
    api = drop_unnecessary_columns(api)
    logging.info("Unnecessary columns dropped successfully.")
    
    api = renombrar_columnas(api)
    logging.info("Renombrar columna municipio successful.")
    
    api = dates_api(api)
    logging.info("Dates converted successfully.")
    
    logging.info("All transformations applied successfully.")
    return api

