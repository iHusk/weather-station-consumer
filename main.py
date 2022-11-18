from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file

from kafka import KafkaConsumer

import pandas as pd

import json

def wind_direction(r):
    '''
    Takes in resistance value and returns wind direction
    '''
    

    if r <= 200:
        return 'E'
    elif r > 200 & r <= 300:
        return 'SE'
    elif r > 300 & r <= 440:
        return 'S'
    elif r > 440 & r <= 750:
        return 'NE'
    elif r > 750 & r <= 1380:
        return 'SW'
    elif r > 1380 & r <= 2850:
        return 'N'
    elif r > 2850 & r <= 5000:
        return 'NE'
    elif r > 5000 & r <= 10000:
        return 'E'

def c_to_f(c):
    return (((9*c)/5)+32)


@task
def process_data(df):

    df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
    df['temperature'] = df['temperature'].apply(lambda x: c_to_f(x))
    df['wind_direction'] = df['wind_direction'].apply(lambda x: wind_direction(x))

    return df


@flow(task_runner=SequentialTaskRunner)
def main_flow():
    logger = get_run_logger()

    logger.info("Creating Consumer...")
    try:
        consumer = KafkaConsumer(
            '20221111-test', 
            group_id='test',
            bootstrap_servers=['192.168.0.25:9092'], 
            consumer_timeout_ms=100, 
            enable_auto_commit=True, 
            auto_offset_reset='latest',
            client_id='master-pi',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    except Exception as e:
        logger.error("Error while creating consumer!!!")
        logger.warning(e)
    
    

    logger.info("Caching data...")
    df = pd.DataFrame()
    try:
        for message in consumer:
            datetime = message.value['datetime']
            rain = message.value['rain']
            wind = message.value['wind']
            wind_direction = message.value['wind_direction']
            temperature = message.value['tmp_temp']
            pressure = message.value['pressure']
            humidity = message.value['humidity']

            payload = [datetime, rain, wind, wind_direction, temperature, pressure, humidity]

            temp = pd.DataFrame([payload])

            df = pd.concat([df, temp], ignore_index=True)
    except Exception as e:
        logger.error("Error while caching data!!!")
        logger.warning(e)

    df.columns = ['datetime', 'rain', 'wind', 'wind_direction', 'temperature', 'pressure', 'humidity']
    consumer.close()

    logger.info("Processing data...")
    try:
        df = process_data(df)
    except Exception as e:
        logger.error("Error while processing data!!!")



if __name__ == "__main__":
    main_flow()