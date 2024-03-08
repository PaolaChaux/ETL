# ETL
### Presented by Paola Chaux Campo - Steven Lopez Vega - Diego Moreno Valencia, students of Autonoma de Occidente University 

## Table of contents
### 1. Description Proyect Water Quality
### 2. Quick data overview
### 3. Objective
### 4. Brief description of what was done
### 5. Requeriments:
### 6. Features
### 7. Installation Steps


## 1. Description Proyect Water Quality
### This project focuses on analyzing the Water Quality Index of Colombia (IRCA) data to provide insights into the state of water quality across various regions in the country. The analysis involves extracting, transforming, and loading (ETL) processes to clean and structure the data for further examination.

## 2. Quick data overview
### The data set that will be used for this project is based on Indicators of the Water Quality Index of Colombia (IRCA) of various pollutants. For this, we have a dataset of a total of 408,312 rows and 8,195,781 raw data identified that are divided into the following 22 columns that will be analyzed:
### Año
### Nombre del departamento
### Código del departamento
### Nombre del municipio
### Código del municipio
### IRCA mínimo
### IRCA máximo
### Promedio IRCA 
### Nombre parámetros
### Muestras evaluadas
### Muestras tratadas
### Muestras sin tratar
### Número de parámetros mínimo
### Número de parámetros máximo
### Número de parámetros promedio
### Número de muestras
### Muestras no aptas
### Porcentaje de no aptas
### Resultado mínimo
### Resultado máximo
### Resultado promedio
### Código


## 3. Objective
### The primary objective of this project is to assess and visualize the water quality in different regions of Colombia, identifying areas with poor water quality and potential pollutants affecting the IRCA.

## 4. Brief description of what was done
### The project involved several stages, including data cleaning, transformation, and analysis. We employed various Python libraries and tools such as pandas, seaborn, and Power BI to manipulate the data and create visualizations that highlight key findings.

## 5. Requeriments:
### Pandas.
### Psycopg2.
### Json.
### Datetime.
### Powerbiclient. 
### Numpy.
### Matplotlib.pyplot.
### Seaborn.
### Python.

## 6. Features
### (Habla sobre las características principales, Aprovecha esta sección para contarle al público cuáles son las características más importantes del proyecto, presúmelas.)

## 7. Installation Steps
### 1. Clone the repository.
### 2. Open the proyect with Visual Studio Code.
### 3. Run the app and enjoy it.

## 8. Considerations
### To establish the connection to the database in postgres, it´s necessary to have a file named "db_config.json" that contain your database credentials in json format for more security and for ease, this file should include: "localhost" of the server address, "user", you username, "password", the password of you postgres and "database" for the specific database that you want to access.
### Also, in this repository the Power BI Client tool was used to give us a report and we edit it to our liking.
### Don't forget to set the environment as kernel before you run the notebook.

