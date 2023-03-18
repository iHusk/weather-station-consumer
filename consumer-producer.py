import quixstreams as qx
import pandas as pd


client = qx.KafkaStreamingClient('127.0.0.1:9092')

print("Opening consumer and producer topics")

topic_consumer = client.get_topic_consumer("quickstart-events")
topic_producer = client.get_topic_producer("output-topic")

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print(df) 
    print('Data transformed') # Transform your data here
    # write data to output topic
    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(df)

# read streams
def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

topic_consumer.on_stream_received = on_stream_received_handler

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

# Handle graceful exit
qx.App.run()