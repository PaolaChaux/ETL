# ETL
<h1 align="center"> Proyecto 2 Entrega Etl  </h1>
<p align="left">
   <img src="https://img.shields.io/badge/STATUS-FINISHED-green">
   </p>

### Presented by Paola Chaux Campo - Steven Lopez Vega - Diego Moreno Valencia, students of Autonoma de Occidente University 

## Table of contents
### 1. Description Proyect Water Quality
### 2. Quick data overview
### 3. Objective
### 4. Brief description of what was done
### 5. Requeriments:
### 6. Features
### 7. Installation Steps
### 8. Considerations

## 1. Description Proyect Water Quality
 This project focuses on analyzing the Water Quality Index of Colombia (IRCA) data to provide insights into the state of water quality across various regions in the country. The analysis involves extracting, transforming, and loading (ETL) processes to clean and structure the data for further examination.

### For this project, a cleaning and transformation of the data was performed to create a star schema consisting of 1 fact table and 4 dimensions:
* Irca_measurements: In the center of the schema we have the fact table, which stores records of water quality measurements from maximum to minimum in the IRCA variable and the record of the number of parameters used for the analysis of harmful bodies in the water. It also has the identifiers of each parameter analyzed, location and year of the study, as well as other necessary variables. This table is surrounded by dimensions that provide additional context to each measurement. 
* Dimension_location: This dimension focuses on geographic attributes such as the name of the department and municipality, which is crucial for geographic analysis and for identifying areas with specific water quality problems.
* Dimension_date: This is in charge of storing the temporal data of each intake and analysis that will facilitate comparisons in the analysis and evidence of improvements or affectations over time recorded.
* Dimension_range: This categorizes the measurements according to quality ranges, respectively in order to facilitate the identification and magnitude of the parameters.
* Dimension_parameters': This refers to the specific parameters analyzed in each measurement, such as pH, turbidity, presence of coliforms, among others. Because of this, this design facilitates complex queries and multidimensional analysis, allowing us to understand trends in water quality over time, by location and quality category, as well as to identify correlations between different water quality parameters.

### Then we proceed to load the data according to our model design to the database in Postgress and the results are connected from the database to our Dashboard.
### As a second part of the project we searched for an Api related to our project in order to extract data that could be useful for more dimensionality to our project.

## 2. Quick data overview
The data set that will be used for this project is based on Indicators of the Water Quality Index of Colombia (IRCA) of various pollutants. For this, we have a dataset of a total of 408,312 rows and 8,195,781 raw data identified that are divided into the following 22 columns that were analyzed, this dataset is the original data:
* Año
* Nombre del departamento
* Código del departamento
* Nombre del municipio
* Código del municipio
* IRCA mínimo
* IRCA máximo
* Promedio IRCA 
* Nombre parámetros
* Muestras evaluadas
* Muestras tratadas
* Muestras sin tratar
* Número de parámetros mínimo
* Número de parámetros máximo
*  Número de parámetros promedio
* Número de muestras
* Muestras no aptas
* Porcentaje de no aptas
* Resultado mínimo
* Resultado máximo
* Resultado promedio
* Código

### After the transformations the most important data we have are:

<span>![</span><span>Modelo Dimensional - Esquema de estrella </span><span>]</span><span>(</span><span>https://github.com/PaolaChaux/ETL-Proyect/blob/main/Screenshot%202024-04-17%20195442.png</span><span>)</span>

## 3. Objective

The primary objective of this project is to assess and visualize the water quality in different regions of Colombia, identifying areas with poor water quality and potential pollutants affecting the IRCA.

## 4. Brief description of what was done

The project involved several stages, including data cleaning, transformation, and analysis. We employed various Python libraries and tools such as pandas, seaborn, and Power BI to manipulate the data and create visualizations that highlight key findings.

## 5. Requeriments:

* Pandas.
* Psycopg2.
* Json.
* Datetime.
* Powerbiclient. 
* Numpy.
* Matplotlib.
* pyplot.
* Seaborn.
* Python.
* Jupiter Notebook.

## 6. Features

Comprehensive Data Analysis: We utilize advanced data analysis techniques to thoroughly assess water quality in Colombia, identifying significant patterns and trends.

Interactive Visualizations: We employ tools such as Matplotlib, Seaborn, and Power BI to create interactive visualizations that facilitate the understanding of data and highlight key findings.

ETL Processes: We implement Extract, Transform, and Load (ETL) processes to clean and structure data, ensuring that the information is accurate and ready for analysis. the dimensional model was made and the tramformations were carried out.

Regional Insights: We provide detailed insights into water quality across different regions of Colombia, enabling authorities and the public to make informed decisions to improve water management.

Pollutant Identification: We identify pollutants that significantly affect the Water Quality Risk Index (IRCA) and highlight areas that require priority attention.

User-friendly Interface: We design an easy-to-use interface that allows users to interact with the data and explore the results of the analysis intuitively.

Data-Driven Decision Making: Our project facilitates data-driven decision-making, providing valuable insights for planning and implementing water quality improvement strategies.

## 7. Installation Steps

### 1. Clone the repository.
### 2. Open the proyect with Visual Studio Code.
#### 3. Create a virtual environment from your terminal: "python -m venv [environment_name]"
#### 4. Activate your virtual environment: "[environment_name]/Scripts/activate"
#### 5. Install the required tools and modules in the environment.
#### 6.Set the created environment as kernel.
### 3. Run the app and enjoy it.

## 8. Considerations
To establish the connection to the database in postgres, it´s necessary to have a file named "db_config.json" that contain your database credentials in json format for more security and for ease, this file should include: "localhost" of the server address, "user", you username, "password", the password of you postgres and "database" for the specific database that you want to access.
Also, in this repository the Power BI Client tool was used to give us a report and we edit it to our liking.
Don't forget to set the environment as kernel before you run the notebook.

### NOTE:
### For the second version project, the files are:
* ETL-Proyect/Main_Aqua_Quality/EDA_water_quality.ipynb..-> EDA 
* ETL-Proyect/airflow_dag/db.py..........................-> Conection to BD, Create BD, Create Star Schema and load the data to BD.
* ETL-Proyect/airflow_dag/transform.py...................-> Transformations Segunda Entrega.
* ETL-Proyect/API/.......................................-> EDA API, Extract data of API and Dashboard API.
* ETL-Proyect/airflow_dag/Proyecto ETL - 2 Entrega.pdf...-> Dashboard general proyect.




