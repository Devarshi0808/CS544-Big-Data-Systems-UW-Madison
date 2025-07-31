from kafka import KafkaAdminClient, KafkaConsumer
import argparse
import time

Stations={'StationA',
          'StationB',
          'StationC',
          'StationD',
          'StationE',
          'StationF',
          'StationG',
          'StationH',
          'StationI',
          'StationJ'}


def is_kafka_up(url):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=url)
        consumer = KafkaConsumer(bootstrap_servers=url)
        print('Kafka is up')
        return True
    except Exception as e:
        print('Kafka is not up yet')
        return False
    
def topics_created(url):
    for _ in range(60):
        admin_client = KafkaAdminClient(bootstrap_servers=url)
        try:
            if "temperatures" in set(admin_client.list_topics()):
                break
        except Exception as e:
            pass
        time.sleep(1)
    else:
        print(f"Expected topics: 'temperatures', Found: {admin_client.list_topics()}")
        return False

    # Fetch topic details
    topic_details = admin_client.describe_topics(["temperatures"])

    # Check details for each topic
    for topic in topic_details:
        topic_name = topic["topic"]
        partitions = len(topic["partitions"])

        # Expected values
        expected_partitions = 4

        # Check and print the details
        if partitions != expected_partitions:
            print(f"Topic '{topic_name}' has incorrect partition count: Expected:{expected_partitions}, Found:{partitions}")
            return False
    
    print('Topics created successfully')


def producer_messages(url):
    global Stations
    consumer = KafkaConsumer(
        bootstrap_servers=url, auto_offset_reset="earliest"
    )
    consumer.subscribe(['temperatures'])

    time.sleep(10)  # Producer should be running, so wait for some data

    batch = consumer.poll(1000)

    if len(batch.items()) == 0:
        print("Was expecting messages in 'temperatures' stream but found nothing")
        return False

    for topic, messages in batch.items():
        if len(messages) == 0:
            print("Was expecting messages in 'temperatures' stream but found nothing")
            return False

        for message in messages:
            if str(message.key, "utf-8") not in Stations:
                print(f"Key must be a station name (first letters capitalized), instead got: {message.key}")
                return False
            
    print('Messages produced successfully')

    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka test helper')
    parser.add_argument('-u', '--url', type=str, help='Kafka URL')
    parser.add_argument('-f', '--function', type=str, help='Function to test')
    args = parser.parse_args()

    if args.function == 'is_kafka_up':
        is_kafka_up(args.url)
    elif args.function == 'topics_created':
        topics_created(args.url)
    elif args.function == 'producer_messages':
        producer_messages(args.url)
