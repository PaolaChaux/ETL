import pandas as pd
import logging

def replace_commas(df):
    """ Reemplaza comas por puntos en la columna 'IrcaPromedio' y convierte a float. """
    df['IrcaPromedio'] = df['IrcaPromedio'].str.replace(',', '.').astype(float)
    logging.info("Commas replaced and converted to float in 'IrcaPromedio'")
    return df

def classify_irca(df):
    """ Clasifica los valores en la columna 'IrcaPromedio' en categorías de riesgo. """
    def clasificar_irca(irca):
        try:
            if not isinstance(irca, float):  # Asegurar que irca es un float
                irca = float(irca.replace(',', '.'))
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
        except ValueError:
            return 'No clasificado'
    df['rango_irca'] = df['IrcaPromedio'].apply(clasificar_irca)
    logging.info("Irca values classified")
    return df

def categorize_treatment(df):
    """ Categoriza el tratamiento de agua basado en 'MuestrasTratadas' y 'MuestrasEvaluadas'. """
    def categorize(row):
        if row['MuestrasTratadas'] == 0:
            return 'Sin tratamiento'
        elif row['MuestrasTratadas'] == row['MuestrasEvaluadas']:
            return 'Tratamiento completo'
        else:
            return 'Tratamiento parcial'
    df['TratamientoCategoría'] = df.apply(categorize, axis=1)
    logging.info("Treatment data categorized")
    return df

def drop_unnecessary_columns(df):
    """ Elimina columnas innecesarias del DataFrame. """
    columns_to_drop = ['ResultadoMinimo', 'ResultadoMaximo', 'ResultadoPromedio',
                       'MuestrasTratadas', 'MuestrasEvaluadas', 'MuestrasSinTratar',
                       'NumeroParametrosMinimo', 'NumeroParametrosMaximo']
    df.drop(columns=columns_to_drop, inplace=True)
    logging.info(f"Columns dropped: {columns_to_drop}")
    return df

def standardize_column_names(df):
    """ Estandariza los nombres de las columnas, reemplazando espacios por guiones bajos y convirtiendo a minúsculas. """
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    logging.info("Column names standardized")
    return df

# Example usage within a data processing script:
# df = pd.read_csv('path/to/data.csv')
# df = replace_commas(df)
# df = classify_irca(df)
# df = categorize_treatment(df)
# df = drop_unnecessary_columns(df)
# df = standardize_column_names(df)
