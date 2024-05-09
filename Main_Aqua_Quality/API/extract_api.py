import pandas as pd
from sodapy import Socrata

client = Socrata("www.datos.gov.co", None)
results = client.get("tcwu-r53g", limit=2000)
results_df = pd.DataFrame.from_records(results)

# Guardar el DataFrame en un archivo CSV
results_df.to_csv("datos_descargados.csv", index=False)

print("Archivo descargado con éxito.")
