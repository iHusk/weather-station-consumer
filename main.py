from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file

from kafka import KafkaConsumer

import pandas as pd
import numpy as np
from datetime import datetime as dt

import json

ARCHIVE_PATH = '/home/pi/python/weather-station-consumer/data/archive'
LIVE_PATH = '/home/pi/python/weather-station-consumer/data/weather-station-live.csv'

def wind_direction(r):
    '''
    Takes in resistance value and returns wind direction
    '''
    

    if r <= 200:
        return 'E'
    elif 200 < r <= 300:
        return 'SE'
    elif 300 < r <= 440:
        return 'S'
    elif 440 < r <= 750:
        return 'NE'
    elif 750 < r <= 1380:
        return 'SW'
    elif 1380 < r <= 2850:
        return 'N'
    elif 2850 < r <= 5000:
        return 'NE'
    elif 5000 < r <= 10000:
        return 'E'

def c_to_f(c):
    '''
    Takes in celcius and returns fahrenheit
    '''
    return (((9*c)/5)+32)


@task
def process_data(df):
    '''
    Converts epoch time to datetime, celcius to fahrenheit, and assigns a wind direction. 
    '''
    logger = get_run_logger()

    logger.info('Processing dates...')
    df['datetime'] = pd.to_datetime(df['datetime'], unit='s')

    logger.info('Processing temps...')
    df['temperature'] = df['temperature'].apply(lambda x: c_to_f(x))

    logger.info('Cowying wind direction...')
    df['wind_direction_raw'] = df['wind_direction'].copy()

    logger.info('Assigning wind direction...')
    df['wind_direction'] = df['wind_direction'].apply(lambda x: wind_direction(x))

    return df


@flow(task_runner=SequentialTaskRunner)
def weather_station_consumer_flow():
    logger = get_run_logger()

    logger.info("Creating Consumer...")

    ## Here we connect to the Kafka Server as a consumer. This gets up all of the data from the last time that we
    ## we requested data **as this consumer**. We can create a different consumer and have a different offset if 
    ## needed. We can also use the flag from beginning to get data from inception. 

    ## TODO: New topic and group_id for production. 
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
    
    ## Here we are putting the data into a pandas dataframe for processing.

    logger.info("Caching data...")

    columns = ['datetime', 'rain', 'wind', 'wind_direction', 'temperature', 'pressure', 'humidity', 'wind_direction_raw']
    df = pd.DataFrame(columns=columns)
    
    try:
        for message in consumer:
            datetime = message.value['datetime']
            rain = message.value['rain']
            wind = message.value['wind']
            wind_direction = message.value['wind_direction']
            ## Want to also have the resistance value in case the logic above ends up being incorrect. 
            ## This was we can go back and audit/fix and errors. 
            wind_direction_raw = message.value['wind_direction']
            temperature = message.value['tmp_temp']
            pressure = message.value['pressure']
            humidity = message.value['humidity']

            payload = [datetime, rain, wind, wind_direction, temperature, pressure, humidity, wind_direction_raw]

            temp = pd.DataFrame([payload])

            df = pd.concat([df, temp], ignore_index=True)
    except Exception as e:
        logger.error("Error while caching data!!!")
        logger.warning(e)


    ## Very important!!
    consumer.close()   

    logger.info("Processing data...")
    try:
        df = process_data(df)
    except Exception as e:
        logger.error("Error while processing data!!!")

    logger.info('Writing data...')
    try:
        ## Doing this monthly might be too long...worried about how long it will take to open file?
        df.to_csv(path_or_buf=f'{ARCHIVE_PATH}/{dt.now().strftime("%Y%m")}.csv',index=False, mode='a+', header=False)

        ## Append to the live data.
        ## TODO: We should also trim the beginning rows that are older than x days old. Or limit how many lines are in the file? 
        df.to_csv(path_or_buf=LIVE_PATH,index=False, mode='a+', header=False)
    except Exception as e:
        logger.error("Error while writing data!!!")
        logger.warning(e)


if __name__ == "__main__":
    weather_station_consumer_flow()