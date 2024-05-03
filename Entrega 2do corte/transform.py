import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_irca_columns(water):
    """Convertir las columnas de IRCA a tipo flotante después de reemplazar las comas."""
    water['IrcaMinimo'] = water['IrcaMinimo'].str.replace(',', '.').astype(float)
    water['IrcaMaximo'] = water['IrcaMaximo'].str.replace(',', '.').astype(float)
    water['IrcaPromedio'] = water['IrcaPromedio'].str.replace(',', '.').astype(float)
    return water

def standardize_place_names(water):
    """Estandarizar los nombres de departamentos y municipios."""
    water['NombreDepartamento'] = water['NombreDepartamento'].str.title().str.strip()
    water['NombreMunicipio'] = water['NombreMunicipio'].str.title().str.strip()
    return water

def scale_columns(water):
    """Escalar las columnas de muestras usando MinMaxScaler."""
    scaler = MinMaxScaler()
    columns_to_scale = ['MuestrasEvaluadas', 'MuestrasTratadas', 'MuestrasSinTratar']
    water[columns_to_scale] = scaler.fit_transform(water[columns_to_scale])
    return water

def standardize_column_names(water):
    """Estandarizar los nombres de columnas a minúsculas y sin espacios."""
    water.columns = water.columns.str.lower().str.replace(' ', '_')
    logging.info("Column names standardized")
    return water

def filter_top_parameters(water):
    """Filtrar los datos para incluir solo los 15 parámetros más influyentes en el IRCA promedio."""
    parametros_influencia = water.groupby('NombreParametroAnalisis2')['IrcaPromedio'].mean().sort_values(ascending=False)
    top_15_parametros = parametros_influencia.head(15)
    return water[water['NombreParametroAnalisis2'].isin(top_15_parametros.index)]

def classify_irca(water):
    """Clasificar los valores de IRCA en categorías de riesgo."""
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
    water['rango_irca'] = water['IrcaPromedio'].apply(clasificar_irca)
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
    water['TratamientoCategoría'] = water.apply(categorize, axis=1)
    return water

def drop_unnecessary_columns(water):
    """Eliminar columnas que no son necesarias para el análisis."""
    columns_to_drop = ['MuestrasTratadas', 'MuestrasEvaluadas', 'MuestrasSinTratar',
                       'NumeroParametrosMinimo', 'NumeroParametrosMaximo', 'ResultadoMinimo', 'ResultadoMaximo', 'ResultadoPromedio']
    return water.drop(columns=columns_to_drop)

def apply_transformations(water):
    """Aplicar todas las transformaciones en el orden correcto."""
    logging.info("Starting transformations on water data.")
    water = convert_irca_columns(water)
    logging.info("Converted IRCA columns to float.")
    
    water = standardize_place_names(water)
    logging.info("Standardized place names.")
    
    water = scale_columns(water)
    logging.info("Scaled numerical columns.")
    
    water = standardize_column_names(water)
    logging.info("Standardized column names.")
    
    water = filter_top_parameters(water)
    logging.info("Filtered top influential parameters.")
    
    water = classify_irca(water)
    logging.info("Classified IRCA values into categories.")
    
    water = categorize_treatment(water)
    logging.info("Categorized treatment data.")
    
    water = drop_unnecessary_columns(water)
    logging.info("Dropped unnecessary columns.")
    
    logging.info("All transformations applied successfully.")
    return water

        
    













#Profe, apartir de aqui estaremos añadiendo nuevas cosas para el 3er corte 
# tranformaciones API:

# def convert_dates(df):
#     df['fecha_terminacion_proyecto'] = pd.to_datetime(df['fecha_terminacion_proyecto'])
#     df['fecha_de_corte'] = pd.to_datetime(df['fecha_de_corte'])
#     return df

# def normalize_text_columns(df):
#     str_cols = df.select_dtypes(include=['object']).columns
#     df[str_cols] = df[str_cols].apply(lambda x: x.str.lower().str.strip())
#     return df

# def compute_num_municipios(df):
#     df['num_municipios'] = df['c_digo_divipola_municipio'].apply(lambda x: len(x.split(',')))
#     return df

# def map_regions(df):
#     region_mapping = {
#         'CHOCO': 'Región Pacífica', 'CAUCA': 'Región Pacífica', 'NARIÑO': 'Región Pacífica', 'VALLE DEL CAUCA': 'Región Pacífica',
#         'ARAUCA': 'Región Orinoquía', 'CASANARE': 'Región Orinoquía', 'GUAINÍA': 'Región Orinoquía', 'GUAVIARE': 'Región Orinoquía', 'META': 'Región Orinoquía', 'VICHADA': 'Región Orinoquía',
#         'AMAZONAS': 'Región Amazonía', 'CAQUETA': 'Región Amazonía', 'PUTUMAYO': 'Región Amazonía', 'VAUPES': 'Región Amazonía',
#         'ANTIOQUIA': 'Región Andina', 'BOYACA': 'Región Andina', 'CALDAS': 'Región Andina', 'CUNDINAMARCA': 'Región Andina', 'HUILA': 'Región Andina', 'NORTE DE SANTANDER': 'Región Andina', 'QUINDIO': 'Región Andina', 'RISARALDA': 'Región Andina', 'SANTANDER': 'Región Andina', 'TOLIMA': 'Región Andina',
#         'SAN ANDRES Y PROVIDENCIA': 'Región Insular', 'ATLANTICO': 'Región Caribe', 'BOLIVAR': 'Región Caribe', 'CESAR': 'Región Caribe', 'CORDOBA': 'Región Caribe', 'LA GUAJIRA': 'Región Caribe', 'MAGDALENA': 'Región Caribe', 'SUCRE': 'Región Caribe'
#     }
#     df['departamento'] = df['departamento'].replace({
#         'SAN ANDRES': 'SAN ANDRES Y PROVIDENCIA',
#         'N DE SANTANDER': 'NORTE DE SANTANDER'
#     }).str.upper()
#     df['región'] = df['departamento'].map(region_mapping).fillna('Región Desconocida')
#     return df

# def calculate_financing(df):
#     df['total_financiamiento'] = df['aporte_nacion'] + df['contrapartida']
#     return df

# def calculate_project_duration(df):
#     df['duracion_proyecto_dias'] = (df['fecha_de_corte'] - df['fecha_terminacion_proyecto']).dt.days
#     return df

# def drop_unnecessary_columns(df):
#     df.drop(['fecha_de_corte', 'fecha_terminacion_proyecto', 'contrapartida', 'aporte_nacion'], axis=1, inplace=True)
#     return df

# def transform_api_data(df):
#     df = convert_dates(df)
#     df = normalize_text_columns(df)
#     df = compute_num_municipios(df)
#     df = map_regions(df)
#     df = calculate_financing(df)
#     df = calculate_project_duration(df)
#     df = drop_unnecessary_columns(df)
#     return df

