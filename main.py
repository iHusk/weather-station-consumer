from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file

from kafka import KafkaConsumer

import json

@flow(task_runner=SequentialTaskRunner)
def main_flow():
    logger = get_run_logger()

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
        print(e)
    
    for message in consumer:
        print(message.value['datetime'])

    consumer.close()


if __name__ == "__main__":
    main_flow()