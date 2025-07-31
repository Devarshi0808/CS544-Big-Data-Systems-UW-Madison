import sys
import os
import json
import time
from kafka import KafkaConsumer, TopicPartition
import report_pb2  # Generated from report.proto

BROKER = "localhost:9092"
TOPIC = "temperatures"
SRC_DIR = "/src"  # Directory for storing JSON files

def ensure_directory_exists(directory):
    """Ensure that the directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def load_previous_data(file_path):
    """Load previous data from JSON file."""
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            return json.load(file)
    return {"offset": 0}  # Default offset to 0

def save_data_atomic(file_path, data):
    """Save data atomically to prevent partial writes."""
    temp_file_path = file_path + ".tmp"
    try:
        with open(temp_file_path, "w") as file:
            json.dump(data, file, indent=4)
            file.flush()
            os.fsync(file.fileno())
        os.rename(temp_file_path, file_path)
        print(f"Successfully wrote data to {file_path}")
    except Exception as e:
        print(f"Error writing data to {file_path}: {e}")

def update_statistics(stats, report):
    """Update the statistics for a station ID."""
    station_id = report["station_id"]
    date = report["date"]
    degrees = report["degrees"]

    if station_id not in stats:
        stats[station_id] = {
            "count": 0,
            "sum": 0.0,
            "avg": 0.0,
            "start": date,
            "end": date,
        }

    station_stats = stats[station_id]
    station_stats["count"] += 1
    station_stats["sum"] += degrees
    station_stats["avg"] = station_stats["sum"] / station_stats["count"]
    station_stats["start"] = min(station_stats["start"], date)
    station_stats["end"] = max(station_stats["end"], date)

def main():
    # Ensure the /src directory exists
    ensure_directory_exists(SRC_DIR)

    # Read partitions from command-line arguments
    partitions = list(map(int, sys.argv[1:]))
    if not partitions:
        print("Error: You must specify at least one partition.")
        sys.exit(1)

    # Load previous data and assign partitions
    consumer = KafkaConsumer(bootstrap_servers=[BROKER], enable_auto_commit=False)
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)

    # Load previous offsets and seek to them
    file_paths = {p: f"{SRC_DIR}/partition-{p}.json" for p in partitions}
    stats = {}
    for tp in topic_partitions:
        file_path = file_paths[tp.partition]
        print(f"Using file path for partition {tp.partition}: {file_path}")

        # Ensure JSON file exists or create new data
        data = load_previous_data(file_path)
        saved_offset = data.get("offset", 0)
        consumer.seek(tp, saved_offset)
        stats[tp.partition] = data

        print(f"Assigned partition {tp.partition}, starting at offset {saved_offset}")

    print(f"Assigned to partitions: {partitions}")

    # Consume messages and update statistics
    for message in consumer:
        partition = message.partition
        file_path = file_paths[partition]

        try:
            # Deserialize the protobuf message
            report_proto = report_pb2.Report()
            report_proto.ParseFromString(message.value)
            report = {
                "station_id": report_proto.station_id,
                "date": report_proto.date,
                "degrees": report_proto.degrees,
            }

            print(f"Processing message from partition {partition}: {report}")

            # Update statistics
            partition_stats = stats[partition]
            update_statistics(partition_stats, report)

            # Update the offset using consumer.position()
            current_position = consumer.position(TopicPartition(TOPIC, partition))
            partition_stats["offset"] = current_position
            print(f"Partition {partition}: Current position is {current_position}")

            # Save the data atomically
            save_data_atomic(file_path, partition_stats)

        except Exception as e:
            print(f"Error processing message in partition {partition}: {e}")

if __name__ == "__main__":
    main()
