from kafka import KafkaConsumer
import report_pb2

# Configure consumer
consumer = KafkaConsumer(
    'temperatures',
    bootstrap_servers=['localhost:9092'],
    group_id='debug',
    auto_offset_reset='latest'
)

# Main loop to consume and print messages
for message in consumer:
    # Deserialize the protobuf message
    report = report_pb2.Report()
    report.ParseFromString(message.value)
    
    # Create and print the dictionary
    output = {
        'station_id': report.station_id,
        'date': report.date,
        'degrees': report.degrees,
        'partition': message.partition
    }
    print(output)
