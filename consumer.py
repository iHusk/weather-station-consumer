import quixstreams as qx
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

def wind_dir(r):
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


# Define Prefect task
@task
def consume_kafka_and_process():
    # Client connecting to Kafka instance locally without authentication. 
    client = qx.KafkaStreamingClient('127.0.0.1:9092')
    bq = bigquery.Client()

    # Open the input topic where to consume data from.
    # For testing purposes we remove consumer group and always read from latest data.
    topic_consumer = client.get_topic_consumer("weather-raw", consumer_group="test", auto_offset_reset=qx.AutoOffsetReset.Latest)

    # consume streams
    def on_stream_received_handler(stream_received: qx.StreamConsumer):
        stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler

    # consume data (as Pandas DataFrame)
    def on_dataframe_received_handler(stream: qx.StreamConsumer, df: pd.DataFrame):
        print("Data received...")
        now = datetime.now()
        year = str(now.year)
        fn = now.strftime("%Y-%m-%d")
        
        table = bq.dataset('weather_station').table('raw')
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV

        # df.rename(columns={'timestamp':'datetime'}, inplace=True)

        w_path = os.path.join(os.path.expanduser('~'), 'Data', 'weather-station', 'raw', year, f'{fn}.csv')

        df['wind_comp'] = df['wind_direction'].apply(lambda x: wind_dir(int(x)))
        df['temp_f'] = df['tmp_temp'].apply(lambda x: ((9*float(x))/5)+32)
        df['datetime'] = pd.to_datetime(df['timestamp'])
        df.drop('timestamp', axis=1, inplace=True)

        print("Writing to BigQuery...")
        try: 
            bq.load_table_from_dataframe(df, table, job_config=job_config)
        except Exception as e:
            print(e)
            pass
        
        print("Writing locally...")
        with open(w_path, 'a') as f:
            df.to_csv(f, header=False)

        print("Complete")

    # Hook up events before initiating read to avoid losing out on any data
    topic_consumer.on_stream_received = on_stream_received_handler

    print("Listening to streams. Press CTRL-C to exit.")
    # Handle graceful exit
    qx.App.run()


# Define Prefect flow
@flow(task_runner=SequentialTaskRunner())
def weather_station_processing():
    consume_kafka_and_process()

# Run Prefect flow
if __name__ == "__main__":
    weather_station_processing()