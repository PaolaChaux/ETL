import pandas as pd
import numpy as np
import logging

def convert_irca_columns(df):
    """Convertir las columnas de IRCA a tipo flotante después de reemplazar las comas."""
    df['IrcaMinimo'] = df['IrcaMinimo'].str.replace(',', '.').astype(float)
    df['IrcaMaximo'] = df['IrcaMaximo'].str.replace(',', '.').astype(float)
    df['IrcaPromedio'] = df['IrcaPromedio'].str.replace(',', '.').astype(float)
    return df

def scale_columns(df):
    """Escalar las columnas de muestras usando MinMaxScaler."""
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    columns_to_scale = ['MuestrasEvaluadas', 'MuestrasTratadas', 'MuestrasSinTratar']
    df[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])
    return df

# Función para estandarizar nombres de columnas
def standardize_column_names(df):
    """Estandarizar los nombres de columnas a minúsculas y sin espacios."""
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df


def classify_irca(df):
    """Clasificar los valores de IRCA en categorías de riesgo."""
    def clasificar_irca(irca):
        if irca == 0:
            return 'Sin información'
        elif 0.001 <= irca <= 5:
            return 'Sin riesgo'
        elif 5.001 <= irca <= 14:
            return 'Riesgo bajo'
        elif 14.001 <= irca <= 35:
            return 'Riesgo medio'
        elif 35.001 <= irca <= 80:
            return 'Riesgo alto'
        elif 80.001 <= irca <= 100:
            return 'Inviable sanitariamente'
        else:
            return 'No clasificado'
    df['rango_irca'] = df['IrcaPromedio'].apply(clasificar_irca)
    return df



def drop_columns(df, columns):
    """Eliminar columnas que no son necesarias para el análisis."""
    df.drop(columns=columns, inplace=True)
    return df


def calculate_percentage_treated(df):
    """Calcular el porcentaje de muestras tratadas sobre el total de muestras evaluadas."""
    df['porcentaje_muestras_tratadas'] = (df['MuestrasTratadas'] / df['MuestrasEvaluadas']) * 100
    df['porcentaje_muestras_tratadas'] = df['porcentaje_muestras_tratadas'].fillna(0) 
    return df


def calculate_range_parameters_analyzed(df):
    """Calcular la diferencia entre el número máximo y promedio de parámetros analizados."""
    df['rango_parametros_analizados'] = df['NumeroParametrosMaximo'] - df['NumeroParametrosPromedio']
    return df

















#Profe estamos a<apartir de aqui estaremos añadiendo nuevas cosas para el 3er corte 
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

