from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file

from kafka import KafkaConsumer

import pandas as pd

import json

@flow(task_runner=SequentialTaskRunner)
def data_process_flow(df):
    logger = get_run_logger()




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
            date_converted = message.value['datetime']
            rain = message.value['rain']
            wind = message.value['wind']
            wind_direction = message.value['wind_direction']
            temperature = message.value['tmp_temp']
            pressure = message.value['pressure']
            humidity = message.value['humidity']

            payload = [date_converted, rain, wind, wind_direction, temperature, pressure, humidity]

            temp = pd.DataFrame([payload])

            df = pd.concat([df, temp], ignore_index=True)
    except Exception as e:
        logger.error("Error while caching data!!!")
        logger.warning(e)

    df.columns = ['datetime', 'rain', 'wind', 'wind_direction', 'tmp_temp', 'pressure', 'humidity']
    consumer.close()

    logger.info("Processing data...")
    try:
        data_process_flow(df)
    except Exception as e:
        logger.error("Error while processing data!!!")



if __name__ == "__main__":
    main_flow()