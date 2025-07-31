# P7 (5% of grade): Kafka, Weather Data

## Overview

For this project, imagine a scenario where you are receiving daily
weather data from various weather stations. Your task is to write this data
to a Kafka stream using a *producer* Python program. A *consumer*
Python program consumes data from the stream to produce JSON files
with summary stats, for use on a web dashboard (you don't need to
build the dashboard yourself).

For simplicity, we use a single Kafka broker instead of using a
cluster. A single producer will generate weather data (max
temperature) in an infinite loop at an accelerated rate of 1 day per
0.1 seconds (you can change this during debugging). Finally, consumers
will be different processes, launching from the same Python program.

Learning objectives:
* write code for Kafka producers and consumers
* apply streaming techniques to achive "exactly once" semantics
* use manual and automatic assignment of Kafka topics and partitions

Before starting, please review the [general project directions](../projects.md).

## Clarifications/Correction

* Nov 22: Added Autograder
* Nov 21: Updated Dockerfile
* Nov 26: Changes in `autograde.py`. This **will not effect** any existing submission. We only reorganized the code so that it is reproducible in grading and has less dependencies.
* Nov 26: Released `check_sub.py`

## Container setup

Build a `p7` docker image with Kafka installed using the provided Dockerfile.
Run the Kafka broker in the background using:

```
docker run -d -v ./src:/src --name=p7 p7
```

You'll be creating three programs, `producer.py`, `debug.py`, and
`consumer.py` in the `src` directory mapped into the container.  You
can launch these in the same container as Kafka using: `docker exec -it -w
/src p7 python3 <name_of_program>`.  This will run the program in
the foreground, making it easier to debug.

All the programs you write for this projects will run forever, or
until manually killed.

## Part 1: Kafka Producer

### Topic Initialization

Create a `src/producer.py` that creates a `temperatures` topic with 4
partitions and 1 replica. If the topic already existed, it should
first be deleted.

Feel free to use/adapt the following:

```python
import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1

print("Topics:", admin_client.list_topics())
```

### Weather Generation

Using the provided `weather.py` file, you can infinitely generate
daily weather data starting from 2000-01-01 for some imaginary
stations. For example, this will generate one weather report every 0.1
seconds:

```python
import weather

# Runs infinitely because the weather never ends
for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    print(date, degrees, station_id)
```

Note: The above snippet is just for testing, don't include it in your submission.

Next, instead of printing the weather, create a KafkaProducer to send the
reports to the `temperatures` topic.

For the Kafka message's value, encode the message as a gRPC protobuf.  
For this, you'll need to create a protobuf file `report.proto` in `src` 
with a `Report` message having the following fields, and build it to get 
a `???_pb2.py` file (review P2 for how to do this if necessary):

* string **date** (format "YYYY-MM-DD") - Date of the observation
* double **degrees**: Observed max-temperature on this date
* string **station_id**: Station ID for the generated weather data

### Requirements
1. Use a setting so that the producer retries up to 10 times when `send` requests fail
2. Use a setting so that the producer's `send` calls are not acknowledged until all in-sync replicas have received the data
3. When publishing to the `temperatures` stream, use station ID as the message's `key`
4. Use a `.SerializeToString()` call to convert a protobuf object to bytes (not a string, despite the name)

### Running in Background

When your producer is finished, consider running it in the background
indefinitely:

```
docker exec -d -w /src p7 python3 producer.py
```

## Part 2: Kafka Debug Consumer

Create a `src/debug.py` program that initializes a KafkaConsumer. It
could be in a consumer group named "debug".

The consumer should subscribe to the "temperatures" topic; let the
broker automatically assign the partitions.

The consumer should NOT seek to the beginning. The consumer should
loop over messages forever, printing dictionaries corresponding to
each message, like the following:

```
...
{'station_id': 'StationC', 'date': '2008-12-17', 'degrees': 35.2621, 'partition': 2}
{'station_id': 'StationC', 'date': '2008-12-20', 'degrees': 13.4537, 'partition': 2}
{'station_id': 'StationE', 'date': '2008-12-24', 'degrees': 35.3709, 'partition': 2}
{'station_id': 'StationA', 'date': '2008-07-06', 'degrees': 80.1362, 'partition': 3}
...
```

Use your `debug.py` to verify your producer is writing to the stream
as expected.

## Part 3: Kafka Consumer

Now, you'll write a `src/consumer.py` script to handle partition
assignment and compute statistics on the temperatures topic,
outputting the results.

`consumer.py` should use manual partition assignment.  If it is
launched as `docker exec -it -w /src p7 python3 consumer.py 0 2`, it
should assign partitions 0 and 2 of the `temperatures` topic.  With
arguments `0 1 2 3`, it will read from all the partitions.

Your consumer should calculate weather statistics for each partition
and write each partition N to a file named `partition-N.json` after
each batch.  There are more weather stations than partitions, so some
of the JSON files will have statistics for multiple stations.

Here is an example `partition-N.json` file with two stations:

```json
{
  "StationD": {
    "count": 34127,
    "sum": 1656331612.266097,
    "avg": 48534.34559926442,
    "start": "1990-01-02",
    "end": "2437-05-01"
  },
  "StationB": {
    "count": 34466,
    "sum": 1700360597.4081032,
    "avg": 49334.433859690806,
    "start": "1990-01-07",
    "end": "2437-05-09"
  }
}
```

Each `partition-N.json` file should have a key for each `station_id` seen in that
partition. Under each `station_id`, you have the following statistics describing messages for that station:

* `count`: the number of days for which data is available for the corresponding `station_id`.
* `sum`: sum of temperatures seen so far (yes, this is an odd metric by itself)
* `avg`: the `sum/count`. This is the only reason we record the sum - so we can recompute the average on a running basis without having to remember and loop over all temperatures each time the file is updated
* `start`: the date of the *first* measurement for the corresponding
station id.
* `end`: the date of the *last* measurement for the corresponding
station id.

## Part 4: Consumer Crash Recovery

### Checkpointing

In addition to the station data, your consumer should write partition
read offsets to the JSON files.  You can get this with a
`consumer.position(????)` call.  Write the offset as a top-level key
in your JSON files, alongside station data, like this:

```json
{
  "offset": 123,
  "StationD": {...},
  "StationB": {...}
}
```

### Restart

When your consumer starts, it should check whether JSON files exist
corresponding to its assigned partitions.  If they exist, it should
load that data, and seek to the previous offsets, like this:

```python
consumer.seek(????)
```

Any messages then read at that offset or following should update
previous stats (like count) instead of starting fresh.

If no previous JSON files were written, seek to offset 0 before
reading any messages.

### Atomic Writes

Remember that we're producing the JSON files so somebody else (not
you) can use them to build a web dashboard. What if the dashboard app
reads the JSON file at the same time your consumer is updating the
file?  It's possible the dashboard or plotting app could read an
incomprehensible mix of old and new data.

To prevent such partial writes, the proper technique is to write
a new version of the data to a different file.  For example, say the
original file is `F.txt` -- you might write the new version to
`F.txt.tmp`.  After the new data has been completely written, you can
rename F.txt.tmp to F.txt.  This atomically replaces the file
contents. Anybody trying to read it will see all old data or all new
data.  Here is an example:

```python
path = ????
path2 = path + ".tmp"
with open(path2, "w") as f:
    # TODO: write the data
    os.rename(path2, path)
```

Be sure to write your JSON files atomically.

Note that this only provides atomicity when the system doesn't crash.
If the computer crashes and restarts, it's possible some of the writes
for the new file might only have been buffered in memory, not yet
written to the storage device.  Feel free to read about `fsync` if
you're curious about this scenario.

## Submission

All your code and generated plot should be in a directory named
`src` within your repository.

We should be able to run the following on your submission to build and
run the required image:

```
# To build the image
docker build . -t p7

# To run the kafka broker
docker run -d -v ./src:/src --name=p7 p7

# To run the producer program
docker exec -d -w /src p7 python3 producer.py

# To run the debug program
docker exec -it -w /src p7 python3 debug.py

# To run the consumer program (for partition 0, 2)
docker exec -it -w /src p7 python3 consumer.py 0 2

```

Verify that your submission repo has a structure with at least the
following committed:

```
.
├── Dockerfile
└── src
    ├── producer.py
    ├── debug.py
    ├── consumer.py
    ├── report.proto
    ├── report_pb2.py
    └── weather.py
```

## Testing

<!-- To run the autograder, you'll need to have kafka-python and grpcio-tools installed on your VM. To do so, run the following command:
```
pip uninstall kafka-python --break-system-packages
pip install --break-system-packages git+https://github.com/dpkp/kafka-python.git
```
```
pip3 install grpcio-tools --break-system-packages
```
Afterwards, you can run the autograder using: -->

Simply run the following:

```
python3 autograde.py
```

Note that: this will use the `src/autograde-helper.py` file to interact with Kafka from within the container. So, no need to install `grpc-tools` or `kafka-python` in your VM.

Next, push your code to GitLab. After that, to check the correctness of your submission:

```
python3 check_sub.py
```
