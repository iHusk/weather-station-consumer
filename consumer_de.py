import quixstreams as qx
import pandas as pd
import datetime

# Client connecting to Kafka instance locally without authentication. 
client = qx.KafkaStreamingClient('127.0.0.1:9092')

# Open the input topic where to consume data from.
# For testing purposes we remove consumer group and always read from latest data.
topic_consumer = client.get_topic_consumer("weather-raw", consumer_group="main-consumer", auto_offset_reset=qx.AutoOffsetReset.Latest)

# consume streams
def on_stream_received_handler(stream_received: qx.StreamConsumer):
    stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler

# consume data (as Pandas DataFrame)
def on_dataframe_received_handler(stream: qx.StreamConsumer, df: pd.DataFrame):
    now = datetime.now()
    year, fn = str(now.year), now.strftime("%Y-%m-%d--%H-%M-%S")
    
    print(df.to_string())

    with open('./data/raw.csv', 'a+') as f:
        df.to_csv(f, header=False)

# Hook up events before initiating read to avoid losing out on any data
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")
# Handle graceful exit
qx.App.run()