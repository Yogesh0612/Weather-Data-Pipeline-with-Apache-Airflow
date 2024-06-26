from airflow import DAG
from datetime import timedelta,datetime
import json
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import s3fs

import os 

os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""

def k_to_f(temp):
    temp_f = (temp - 273.15) * (9/5) + 32
    return temp_f



def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids = 'extract_weather_data')
    city = data['name']
    weather_description = data['weather'][0]['description']
    temp_f = k_to_f(data['main']['temp'])
    feels_like_f = k_to_f(data['main']['feels_like'])
    min_temp_f = k_to_f(data['main']['temp_min'])
    max_temp_f =k_to_f(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        'City' : city,
        'Description': weather_description,
        'Temperature (F)' : temp_f,
        'Feels Like (F)' : feels_like_f,
        'Min Temp (F)' : min_temp_f,
        'Max Temp (F)' : max_temp_f,
        'Pressure' : pressure,
        'Humidity' : humidity,
        'Wind Speed' : wind_speed,
        'Time of Record' : time_of_record,
        'Sunrise (Local Time)' : sunrise_time,
        'Sunset (Local Time)' : sunset_time
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_madison_' + dt_string
    df_data.to_csv(f's3://airflow-proj-yog-1/{dt_string}.csv', index = False)


default_args = {
    'owner': 'airflow',
    'depends_on_past' : 'False',
    'start_date' : datetime(2024,1, 8),
    'email' : ['yogeshram081@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes = 2)
}






with DAG(
    'weather_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False) as dag:


    is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=madison&appid=ec5bb53b519f73cff231d3c52599bee9'
    )


    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=madison&appid=ec5bb53b519f73cff231d3c52599bee9',
        method = 'GET',
        response_filter = lambda r : json.loads(r.text),
        log_response = True
    )


    transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable = transform_load_data
    )


    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data