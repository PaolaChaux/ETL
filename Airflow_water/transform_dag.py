import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import logging
import re

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')




def renombrar_columnas_water(water):
    columns_rename = {
        'NumeroParametrosPromedio': 'numero_parametros_promedio',
        'NombreParametroAnalisis2': 'nombre_parametro_analisis',
        'IrcaPromedio': 'irca_promedio',
        'NombreMunicipio': 'nombre_municipio',
        'NombreDepartamento': 'nombre_departamento',
        'Año': 'año',
    }
    water = water.rename(columns=columns_rename)
    return water


def dates_water(water):
    water['Año'] = pd.to_datetime(water['Año'])
    return water

def standardize_place_names(water):
    water['nombre_departamento'] = water['nombre_departamento'].str.title().str.strip()
    water['nombre_municipio'] = water['nombre_municipio'].str.title().str.strip()
    return water

def normalize_text_columns_water(water):
    str_cols = water.select_dtypes(include=['object']).columns
    for col in str_cols:
        water[col] = water[col].astype(str).str.lower().str.strip()
    return water

def scale_columns(water):
    scaler = MinMaxScaler()
    columns_to_scale = ['MuestrasEvaluadas', 'MuestrasTratadas', 'MuestrasSinTratar']
    water[columns_to_scale] = scaler.fit_transform(water[columns_to_scale])
    return water

def filter_top_parameters(water):
    parametros_influencia = water.groupby('nombre_parametro_analisis')['irca_promedio'].mean().sort_values(ascending=False)
    top_20_parametros = parametros_influencia.head(20)
    water['is_top_20'] = water['nombre_parametro_analisis'].isin(top_20_parametros.index)
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
    water['rango_irca'] = water['irca_promedio'].apply(clasificar_irca)
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
    water['tratamiento_categoria'] = water.apply(categorize, axis=1)
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
    water['proporción_crítica'] = water.apply(proportion, axis=1)
    return water

def drop_unnecessary_columns_water(water):
    """Eliminar columnas que no son necesarias para el análisis."""
    columns_to_drop = ['MuestrasTratadas', 'MuestrasEvaluadas', 'MuestrasSinTratar',
                       'NumeroParametrosMinimo', 'NumeroParametrosMaximo', 'ResultadoMinimo', 'ResultadoMaximo', 'ResultadoPromedio','IrcaMaximo', 'IrcaMinimo'  ]
    return water.drop(columns=columns_to_drop)



def transformations_water(water):
    logging.info("Starting transformations on water data.")
    
    
    
    
    
    # Primero, renombrar las columnas para asegurarnos de que los nombres sean consistentes
    
    water = renombrar_columnas_water(water)
    logging.info("Renombrar columnas water successfully.")
    print("Columnas después de renombrar_columnas_water:", water.columns)
    
       # Después, aplicar las transformaciones que dependen de los nuevos nombres
    water = dates_water(water)
    logging.info("Dates converted successfully.")
    print("Columnas después de dates_water:", water.columns)
    
    water = normalize_text_columns_water(water)
    logging.info("Normalize text columns water successfully.")
    print("Columnas después de normalize_text_columns_water:", water.columns)
    
    water = scale_columns(water)
    logging.info("Scaled numerical columns.")
    print("Columnas después de scale_columns:", water.columns)
    
    water = filter_top_parameters(water)
    logging.info("Filtered top influential parameters.")
    print("Columnas después de filter_top_parameters:", water.columns)
    
    water = classify_irca(water)
    logging.info("Classified IRCA values into categories.")
    print("Columnas después de classify_irca:", water.columns)
    
    water = categorize_treatment(water)
    logging.info("Categorized treatment data.")
    print("Columnas después de categorize_treatment:", water.columns)
    
    water = calculate_critical_proportion(water)
    logging.info("Calculated critical proportion.")
    print("Columnas después de calculate_critical_proportion:", water.columns)
    
    water = drop_unnecessary_columns_water(water)
    logging.info("Dropped unnecessary columns.")
    print("Columnas después de drop_unnecessary_columns_water:", water.columns)
    
    
    logging.info("All transformations applied successfully.")
    return water




        
    






# tranformaciones API:


def remove_parentheses(api):
    api['nombre_municipio'] = api['nombre_municipio'].str.replace(r"\(.*?\)", "", regex=True)
    return api

def separate_municipalities(api):
    api = api.assign(municipio=api['nombre_municipio'].str.split(',')).explode('nombre_municipio')
    return api

def space_capitalize(api):
    api['nombre_municipio'] = api['nombre_municipio'].str.strip().str.capitalize()
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
    for col in str_cols:
        # Verificar si cada valor es una cadena antes de aplicar operaciones de cadena
        api[col] = api[col].apply(lambda x: x.lower().strip() if isinstance(x, str) else x)
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
    
    api = renombrar_columnas(api)
    logging.info("Renombrar columna municipio successful.")
    
    api = dates_api(api)
    logging.info("Dates converted successfully.")
    
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
    

    logging.info("All transformations applied successfully.")
    return api
    
    
    
    
 
    
    

