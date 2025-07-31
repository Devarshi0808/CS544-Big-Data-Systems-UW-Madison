import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
import weather
import report_pb2

# Kafka configuration
broker = 'localhost:9092'
topic_name = 'temperatures'
num_partitions = 4
replication_factor = 1

# Set up admin client
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

# Delete topic if it exists
try:
    admin_client.delete_topics([topic_name])
    print(f"Deleted topic: {topic_name}")
except UnknownTopicOrPartitionError:
    print(f"Topic does not exist: {topic_name}")

# Wait for topic deletion to propagate
for attempt in range(10):  # Retry up to 10 times
    if topic_name not in admin_client.list_topics():
        print(f"Topic {topic_name} successfully deleted")
        break
    print(f"Waiting for topic deletion to propagate... (Attempt {attempt + 1}/10)")
    time.sleep(1)
else:
    print(f"Warning: Topic {topic_name} deletion not fully propagated, proceeding anyway")

# Create new topic
try:
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    admin_client.create_topics([topic])
    print(f"Created topic: {topic_name}")
except TopicAlreadyExistsError:
    print(f"Topic already exists: {topic_name}, skipping creation")
except Exception as e:
    print(f"Error creating topic {topic_name}: {e}")

# List all topics for verification
print("Current topics:", admin_client.list_topics())

# Create producer with specified configurations
producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,  # Retry up to 10 times
    acks='all',  # Wait for all in-sync replicas to acknowledge
)

# Main loop to generate and send weather data
try:
    print(f"Starting to produce messages to topic: {topic_name}")
    for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
        # Create protobuf message
        report = report_pb2.Report()
        report.date = date
        report.degrees = degrees
        report.station_id = station_id
        
        # Serialize and send message
        serialized_report = report.SerializeToString()
        producer.send(
            topic_name,
            key=station_id.encode('utf-8'),
            value=serialized_report
        )
        producer.flush()
        print(f"Sent data to topic {topic_name}: {date}, {degrees}, {station_id}")
except Exception as e:
    print(f"Error during message production: {e}")
finally:
    print("Closing producer")
    producer.close()
