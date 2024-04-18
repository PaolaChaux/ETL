import pandas as pd
import numpy as np
import logging

# Función para reemplazar comas y convertir a float
def convert_irca_columns(df):
    """Convertir las columnas de IRCA a tipo flotante después de reemplazar las comas."""
    df['IrcaMinimo'] = df['IrcaMinimo'].str.replace(',', '.').astype(float)
    df['IrcaMaximo'] = df['IrcaMaximo'].str.replace(',', '.').astype(float)
    df['IrcaPromedio'] = df['IrcaPromedio'].str.replace(',', '.').astype(float)
    return df

# Función para escalar columnas
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

# Función para clasificar IRCA
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

# Función para categorizar el tratamiento
def categorize_treatment(df):
    """Categorizar el tratamiento de agua basado en muestras tratadas y evaluadas."""
    def categorize(row):
        if row['MuestrasTratadas'] == 0:
            return 'Sin tratamiento'
        elif row['MuestrasTratadas'] == row['MuestrasEvaluadas']:
            return 'Tratamiento completo'
        else:
            return 'Tratamiento parcial'
    df['TratamientoCategoría'] = df.apply(categorize, axis=1)
    return df

# Función para eliminar columnas no necesarias
def drop_columns(df, columns):
    """Eliminar columnas que no son necesarias para el análisis."""
    df.drop(columns=columns, inplace=True)
    return df

# Ejemplo de uso en un script:
if __name__ == "__main__":
    # Carga de datos
    df = pd.read_csv('path_to_your_data.csv', delimiter=';')
    # Aplicar transformaciones
    df = convert_irca_columns(df)
    df = scale_columns(df)
    df = standardize_column_names(df)
    df = classify_irca(df)
    df = categorize_treatment(df)
    df = drop_columns(df, ['ResultadoMinimo', 'ResultadoMaximo', 'ResultadoPromedio', 'MuestrasTratadas', 'MuestrasEvaluadas', 'MuestrasSinTratar'])

