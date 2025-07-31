# ================== tester.py ==================
from collections import OrderedDict
import json
import argparse
import os
import traceback
import shutil
import multiprocessing

multiprocessing.set_start_method("fork")

def warn(msg):
    print(f"🟡 Warning: {msg}")


def error(msg):
    print(f"🔴 Error: {msg}")


def info(msg):
    print(f"🔵 Info: {msg}")


VERBOSE = False
TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None
DEBUG = None
GO_FOR_DEBUG = None
PROJECT_REMOTE_URL = (
    "https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/raw/main/p7/"
)


# dataclass for storing test object info


class _unit_test:
    def __init__(self, func, points, timeout, desc):
        self.func = func
        self.points = points
        self.timeout = timeout
        self.desc = desc

    def run(self, ret):
        points = 0

        try:
            result = self.func()
            if not result:
                points = self.points
                result = f"PASS ({self.points}/{self.points})"
        except Exception as e:
            result = traceback.format_exception(e)
            print(f"Exception in {self.func.__name__}:\n")
            print("\n".join(result) + "\n")

        ret.send((points, result))


# init decorator
def init(init_func):
    global INIT
    INIT = init_func
    return init_func


# test decorator
def test(points, timeout=None, desc=""):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(
            test_func, points, timeout, desc)

    return wrapper

# debug dir decorator


def debug(debug_func):
    global DEBUG
    DEBUG = debug_func
    return debug_func

# cleanup decorator


def cleanup(cleanup_func):
    global CLEANUP
    CLEANUP = cleanup_func
    return cleanup_func


# lists all tests
def list_tests():
    for test_name, test in TESTS.items():
        print(f"{test_name}({test.points}): {test.desc}")


# run all tests
def run_tests():
    results = {
        "score": 0,
        "full_score": 0,
        "tests": {},
    }

    for test_name, test in TESTS.items():
        if VERBOSE:
            print(f"===== Running Test {test_name} =====")

        results["full_score"] += test.points

        ret_send, ret_recv = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=test.run, args=(ret_send,))
        proc.start()
        proc.join(test.timeout)
        if proc.is_alive():
            proc.terminate()
            points = 0
            result = "Timeout"
        else:
            (points, result) = ret_recv.recv()

        if VERBOSE:
            print(result)
        results["score"] += points
        results["tests"][test_name] = result

    assert results["score"] <= results["full_score"]
    if VERBOSE:
        print("===== Final Score =====")
        print(json.dumps(results, indent=4))
        print("=======================")
    # and results['score'] != results["full_score"]
    if DEBUG and GO_FOR_DEBUG:
        DEBUG()
    # cleanup code after all tests run
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    return results


# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/test.json"
    print(f"\rOutput written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


def check_files(required_files):
    if not os.path.isdir(f".git"):
        warn(f"This is not a repository")

    missing_files = []
    for file in required_files:
        if not os.path.exists(f"{file}"):
            missing_files.append(file)
    if len(missing_files) > 0:
        msg = ", ".join(missing_files)
        warn(f"the following required files are missing: {msg}")


def check_for_updated_files():
    files = ["autograde.py", "Dockerfile", "src/autograde-helper.py"]
    for file in files:
        # check local existence
        if not os.path.exists(file):
            error(f"{file} is not found in the current directory.")
            continue

        # get md5sum of the remote file
        remote_md5sum, _ = subprocess.run(
            f"wget {PROJECT_REMOTE_URL}{file} -O - | md5sum",
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.split()
        # get md5sum of the local file
        local_md5sum, _ = subprocess.run(
            f"md5sum {file}", shell=True, capture_output=True, text=True
        ).stdout.split()

        if remote_md5sum != local_md5sum:
            print("=" * 40)
            warn(f"{file} is not the same as the file in the repository.")
            print(
                "This could be: (1) You may have modified the file. (2) The file has been updated."
            )
            print("=" * 40)


def download_helper():
    helper = "src/autograde-helper.py"
    if not os.path.exists(helper):
        print(f"Downloading autograde-helper.py for the first time")
        subprocess.run(
            f"wget {PROJECT_REMOTE_URL}{helper} -O {helper}", shell=True
        )


def tester_main():
    global VERBOSE, TEST_DIR, GO_FOR_DEBUG

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dir", type=str, default=".", help="path to your repository"
    )
    parser.add_argument("-l", "--list", action="store_true",
                        help="list all tests")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-g", "--debug", action="store_true",
                        help="create a debug directory with the files used while testing")
    parser.add_argument("-e", "--existing", default=None,
                        help="run the autograder on an existing notebook")
    args = parser.parse_args()

    if args.list:
        list_tests()
        return

    download_helper()
    check_for_updated_files()
    check_files([
        "Dockerfile",
        "src/weather.py",
        "src/producer.py",
        "src/consumer.py",
        "src/debug.py",
        "src/report.proto",
        "src/report_pb2.py",
    ])

    VERBOSE = args.verbose
    GO_FOR_DEBUG = args.debug
    test_dir = args.dir
    if not os.path.isdir(test_dir):
        print("invalid path")
        return
    TEST_DIR = os.path.abspath(test_dir)

    # make a copy of the code
    def ignore(_dir_name, _dir_content): return [
        ".git", ".github", "__pycache__", ".gitignore", "*.pyc"]
    shutil.copytree(src=TEST_DIR, dst=TMP_DIR,
                    dirs_exist_ok=True, ignore=ignore)

    if args.existing is None and CLEANUP:
        CLEANUP()

    os.chdir(TMP_DIR)

    # run init
    if INIT:
        INIT(existing_file=args.existing)

    # run tests
    results = run_tests()
    save_results(results)

    # run cleanup
    if args.existing is None and CLEANUP:
        CLEANUP()


# ================== autograde.py ==================
import os
from datetime import datetime
import time
import re
import subprocess
import json
import time
import subprocess
import time
import json

BROKER_URL = "localhost:9092"
AUTOGRADE_CONTAINER = "p7-autograder-kafka"
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


def get_environment():
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"


def log(s):
    print(f"\r|--------------- {s}", flush=True)


def restart_kafka():
    subprocess.call(f"docker kill {AUTOGRADE_CONTAINER}", shell=True)
    subprocess.call(f"docker rm {AUTOGRADE_CONTAINER}", shell=True)
    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--name",
                AUTOGRADE_CONTAINER,
                "-v",
                "./src:/src",
                "-e",
                "AUTOGRADER_DELAY_OVERRIDE_VAL=0.01",
                "-d",
                "p7-autograder-build",
            ],
            check=True,
        )
        if result.returncode != 0:
            return "Failed to run Kafka container"
    except subprocess.CalledProcessError as e:
        return "Failed to run Kafka container"


def wait_for_kafka_to_be_up():
    log(f"Re-starting Kafka for new test (waits up to 45 sec)...")
    for _ in range(45):
        result = subprocess.run(
            [
                "docker",
                "exec",
                AUTOGRADE_CONTAINER,
                "python3",
                "/src/autograde-helper.py",
                "-u",
                BROKER_URL,
                "-f",
                "is_kafka_up",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        output = result.stdout.decode("utf-8").strip()
        print(output)
        if "Kafka is up" in output:
            break
        time.sleep(1)
    else:
        raise Exception("Failed to start Kafka")


def run_producer():
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-d",
                AUTOGRADE_CONTAINER,
                "python3",
                "/src/producer.py",
            ],
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to run producer script")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run producer script:" + str(e))


def run_consumer(partition_nums):
    args = [
                "docker",
                "exec",
                "-d",
                AUTOGRADE_CONTAINER,
                "python3",
                "/src/consumer.py",
            ]
    for p in partition_nums: args.append(str(p))
    try:
        result = subprocess.run(
            args,
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to run consumer script for partitions:", partition_nums)
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run consumer script:" + str(e) + "partitions:", partition_nums)


# def delete_temp_dir():
#     global TMP_DIR
#     log(f"Cleaning up temp dir '{TMP_DIR}'")
#     subprocess.check_output(f"rm -rf {TMP_DIR}", env=get_environment(), shell=True)


# def create_temp_dir():
#     global TMP_DIR
#     log(f"Creating temp dir '{TMP_DIR}'")
#     os.makedirs(TMP_DIR, exist_ok=True)


def save_cmd_output(command, output_file, duration=5):
    output_file = os.path.join(os.getcwd(), output_file)
    with open(output_file, "w") as file:
        process = subprocess.Popen(
            command, shell=True, stdout=file, stderr=subprocess.STDOUT
        )

    time.sleep(duration)
    process.terminate()


def read_file_from_docker(container_name, file_path):
    command = f"docker exec {container_name} cat {file_path}"
    result = subprocess.run(
        command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def run_in_docker(container_name, command):
    command = f"docker exec {container_name} {command}"
    result = subprocess.run(
        command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout

def is_day_count_valid(data):
    date2 = datetime.strptime(data['end'], "%Y-%m-%d")
    date1 = datetime.strptime(data['start'], "%Y-%m-%d")
    #date1 = datetime(date2.year, date2.month, 1)
    delta = (date2 - date1).days + 1
    return data['count'] == delta

@cleanup
def _cleanup(*args, **kwargs):
    log("Cleaning up: Stopping all existing containers and temp files")
    subprocess.call(f"docker kill {AUTOGRADE_CONTAINER}", shell=True)
    subprocess.call(f"docker rm {AUTOGRADE_CONTAINER}", shell=True)


@init
def init(*args, **kwargs):
    pass


# Test all required files present
@test(5)
def test_all_files_present():
    files_dir = "src/"
    expected_files = ["Dockerfile"] + [
        files_dir + p
        for p in (
            "producer.py",
            "consumer.py",
            "debug.py",
            "report.proto",
            "weather.py",
            "report_pb2.py",
        )
    ]

    for file_name in expected_files:
        if not os.path.exists(file_name):
            return "Couldn't find file " + str(file_name)

    return None


# Test p7 image builds
@test(5)
def test_p7_image_builds():
    log("Running Test: build P7 image...")
    try:
        result = subprocess.run(
            ["docker", "build", ".", "-t", "p7-autograder-build"], check=True
        )
        return None if result.returncode == 0 else "Failed to build Dockerfile"
    except subprocess.CalledProcessError as e:
        return "Failed to build Dockerfile"


# Check p7 container runs
@test(5)
def test_p7_image_runs():
    log("Running Test: running P7 container...")
    restart_kafka()

# Check KafkaProducer(..., acks='all', retries=10)
@test(5)
def test_producer_configs():
    with open("src/producer.py", "r") as f:
        producer_content = f.read()
        
        p1 = r'KafkaProducer.*\(.*acks\s*\=\s*all.*retries\s*\=\s*10\)'
        p2 = r'KafkaProducer.*\(.*retries\s*\=\s*10.*acks\s*\=\s*all\)'
        
        match_p1 = re.search(p1, producer_content)
        match_p2 = re.search(p2, producer_content)

        if not match_p1 and match_p2:
            return "Have you set the producers 'acks' and 'retries'? Couldn't find: KafkaProducer(..., acks='all', retries=10) in producer.py"

# Test producer: check all topics created
@test(15)
def test_topics_created():
    log("Running Test: check producer creates all topics...")
    try:
        wait_for_kafka_to_be_up()
    except Exception as e:
        return "Kafka container did not start: " + str(e)

    try:
        run_producer()
    except Exception as e:
        return "Failed to run producer.py:" + str(e)

    result = subprocess.run([
        "docker",
        "exec",
        AUTOGRADE_CONTAINER,
        "python3",
        "/src/autograde-helper.py",
        "-u",
        BROKER_URL,
        "-f",
        "topics_created",
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE)
    output = result.stdout.decode("utf-8").strip()
    print(output)
    if not "Topics created successfully" in output:
        return "Failed: " + output


# test producer as consumer
@test(15)
def test_producer_messages():
    log("Running Test: checking 'temperatures' stream...")
    result = subprocess.run(
        [
            "docker",   
            "exec",
            AUTOGRADE_CONTAINER,
            "python3",
            "/src/autograde-helper.py",
            "-u",
            BROKER_URL,
            "-f",
            "producer_messages",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    output = result.stdout.decode("utf-8").strip()
    print(output)
    if not "Messages produced successfully" in output:
        return "Failed: " + output

# test proto generation
@test(5)
def test_proto_build():
    log("Running Test: testing proto file ...")
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                AUTOGRADE_CONTAINER,
                "python3",
                "-m",
                "grpc_tools.protoc",
                "-I",
                "/src",
                "--python_out",
                "/tmp",
                "/src/report.proto",
            ],
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to compile report.proto")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to compile report.proto:" + str(e))


@test(10)
def test_debug_consumer_output():
    log("Running Test: testing debug.py ...")
    
    time.sleep(10)

    out_file = "q7.out"
    save_cmd_output(
        f"docker exec -it {AUTOGRADE_CONTAINER} python3 /src/debug.py", out_file, 10
    )

    with open(out_file, "r") as file:
        for line in file:
            try:
                data = json.loads(
                    line.replace("'", '"')
                )  # Convert single quotes to double quotes for valid JSON
                if all(key in data for key in ["station_id","date", "degrees","partition", ]):
                    return
                else: return "Invalid keys in the output of debug.py. Keys must be: 'station_id','date','degrees','partition'"
            except Exception as e:
                return "Invalid line in debug.py output: " + str(line)
        return "Couldn't find the expected ouput when running debug.py"


@test(10)
def test_consumer_runs():
    log("Running Test: running consumer ...")

    # Delete parition files inside the container
    for _ in range(10):
        try:
            for i in range(4):
                run_in_docker(AUTOGRADE_CONTAINER, f"rm -rf /src/partition-{i}.json")
            break
        except Exception as e:
            pass
    else:
        return "Failed to setup consumer. Make sure your partition files are in a 'src' dir"

    try:
        run_consumer([0,1,2,3])
    except Exception as e:
        return "Failed to run consumer.py: " + str(e)


@test(10)
def test_partition_json_creation():
    log("Running Test: testing partition files ...")
    global Stations

    stations_seen = set()
    partition_offsets = dict()
    
    time.sleep(60)

    for _ in range(15):
        error_msg = ""
        time.sleep(1)
        try:
            for i in range(4):
                try:
                    file_data = read_file_from_docker(
                        AUTOGRADE_CONTAINER, f"/src/partition-{i}.json"
                    )
                except Exception as e:
                    error_msg = str(e)
                    break
                partition_dict = json.loads(file_data)
                for key in partition_dict:
                    if key not in ("offset"):
                        stations_seen.add(key)
                partition_offsets[str(i)] = partition_dict[
                    "offset"
                ]
            else:
                break
        except Exception as e:
            error_msg = str(e)
            pass
    else: return f"Failed to generate and read /src/partition-{i}.json from within the container: {error_msg}"

    #for station in Stations:
    for station in Stations:
        if station not in stations_seen:#stations_seen:
            return f"No partition JSON has weather summary for {station}"

    for k in partition_offsets:
        if partition_offsets[k] == 0:
            return f"Partition offset of partition number {k} doesn't increase"


# Validate contents of partition files generated
@test(15)
def test_partition_json_contents():
    log("Running Test: validating partition files ...")
    
    for _ in range(10):
        time.sleep(1)
        for i in range(4):
            try:
                file_data = read_file_from_docker(
                    AUTOGRADE_CONTAINER, f"/src/partition-{i}.json"
                )
                partition_dict = json.loads(file_data)
            except Exception as e:
                break
            
            found_a_station = False
            for station in partition_dict:
                if station in ("offset"):
                    continue
                found_a_station = True

                if len(partition_dict[station].keys()) == 0:
                    return f"No weather summary data generated for {station}: Make sure the partition JSON resembles the sample structure"

                for key in {"count", "sum", "avg", "start", "end"}:
                    if key not in partition_dict[station]:
                        return f"{station} doesn't contain the key:{key}"
                    if not is_day_count_valid(partition_dict[station]):
                        return f"{station} has an invalid 'count' when compared to 'start' and 'end' dates"

            if not found_a_station:
                return f"No weather summary data found in partition-{i}.json"
        else: 
            return
    return f"Failed to read /src/partition-{i}.json inside the container."


if __name__ == "__main__":
    tester_main()

