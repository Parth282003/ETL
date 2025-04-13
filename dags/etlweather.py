#weather reallife dag making starting from the weather data
import requests
import json
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task 
from airflow.utils.dates import days_ago



LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'weather_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}
with DAG(dag_id='w_e_p',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,) as dags:
    @task()
    def extract_weather_data():
        #use http hooks to get data 
        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')
        #build the api end point 
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response=http_hook.run(endpoint=endpoint)
        #check if the response is ok
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")
        
    @task
    def transform_weather_data(weather_data):
            # Transform the data to match the database schema
            current_weather = weather_data['current_weather']
            transformed_data={
                'latitude': LATITUDE,
                'longitude': LONGITUDE,
                'temperature': current_weather['temperature'],
                'windspeed': current_weather['windspeed'],
                'winddirection': current_weather['winddirection'], 
                'weathercode': current_weather['weathercode']
            }
            return transformed_data
    @task
    def load_weather_data_to_postgres(weather_data):
            # Load the data into PostgreSQL
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        temperature FLOAT,
        windspeed FLOAT,
        winddirection INT,
        weathercode INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

            cursor.execute(insert_query, (
                weather_data['latitude'],
                weather_data['longitude'],
                weather_data['temperature'],
                weather_data['windspeed'],
                weather_data['winddirection'],
                weather_data['weathercode']
            ))
            conn.commit()
            cursor.close()
            conn.close()
            #dag workflow etl pipleline 
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data_to_postgres(transformed_data)

