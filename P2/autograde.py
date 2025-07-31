# ============= legacy code: tester.py =============

import time
from pathlib import Path
import subprocess
from collections import OrderedDict
import json
import argparse
import os
import traceback
import shutil
import multiprocessing

multiprocessing.set_start_method("fork")

VERBOSE = True

TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None
DEBUG = None
GO_FOR_DEBUG = None

# dataclass for storing test object info


class TestPoint:
    def __init__(self, point, desc=None):
        self.point = point
        self.desc = desc


class _unit_test:
    def __init__(self, func, points, timeout, desc, required_files):
        self.func = func
        self.points = points
        self.timeout = timeout
        self.desc = desc
        self.required_files = required_files

    def run(self, ret):
        points = 0

        # check if required files exist
        for file in self.required_files:
            if not os.path.exists(file):
                result = f"{file} not found"
                ret.send((points, result))
                return

        try:
            result = self.func()
            if not result:
                points = self.points
                result = f"PASS ({self.points}/{self.points})"
            if isinstance(result, TestPoint):
                points = result.point
                if points == self.points:
                    verdict = "PASS"
                elif points == 0:
                    verdict = "FAIL"
                else:
                    verdict = "PARTIAL"

                desc = result.desc
                result = f"{verdict} ({points}/{self.points})"
                if desc:
                    result += f": {desc}"

        except Exception as e:
            result = traceback.format_exception(e)
            print(f"Exception in {self.func.__name__}:\n")
            print("\n".join(result) + "\n")

        if VERBOSE:
            if points == self.points:
                print(f"🟢 PASS ({points}/{self.points})")
            elif points == 0:
                print(f"🔴 FAIL ({points}/{self.points})")
            else:
                print(f"🟡 PARTIAL ({points}/{self.points})")

        ret.send((points, result))


# init decorator
def init(init_func):
    global INIT
    INIT = init_func
    return init_func


# test decorator
def test(points, timeout=None, desc="", required_files=[]):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(
            test_func, points, timeout, desc, required_files)

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
            print(f"===== Running {test_name} =====")

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
    output_file = f"{TEST_DIR}/score.json"
    print(f"Output written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


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
    args = parser.parse_args()

    if args.list:
        list_tests()
        return

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

    if CLEANUP:
        CLEANUP()

    os.chdir(TMP_DIR)

    # run init
    if INIT:
        INIT()

    # run tests
    results = run_tests()
    save_results(results)

    # run cleanup
    if CLEANUP:
        CLEANUP()


# ============= end of legacy code =============
# ============= autograder code =============


AUTOGRADE_NETWORK = "autograde_default"
COMPOSE_PROJECT = "wins"
COMPOSE_NETWORK = f"{COMPOSE_PROJECT}_default"


@cleanup
def _cleanup(*args, **kwargs):
    stop_all_containers()
    docker_prune()


def stop_cluster():
    subprocess.run(["docker", "compose", "down"], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("[CLEANUP] Stopping cluster.")


def stop_all_containers():
    container_ids = subprocess.run(
        ["docker", "ps", "-aq"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    ).stdout.decode('utf-8').strip()

    if container_ids:
        # Stop the containers if any are running
        stop_command = ["docker", "stop"] + container_ids.split()
        result = subprocess.run(
            stop_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop_remove_all_containers():
    container_ids = subprocess.run(
        ["docker", "ps", "-aq"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    ).stdout.decode('utf-8').strip()

    if container_ids:
        # Stop the containers if any are running
        stop_command = ["docker", "stop"] + container_ids.split()
        result = subprocess.run(
            stop_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Remove the containers
        rm_command = ["docker", "rm"] + container_ids.split()
        result = subprocess.run(
            rm_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("[CLEANUP] Removing containers.")


def stop_remove_container(container_name):
    subprocess.run(["docker", "stop", container_name],
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "rm", container_name],
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"[CLEANUP] Removing container '{container_name}'.")


def remove_network(network):
    subprocess.run(["docker", "network", "rm", network],
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"[CLEANUP] Removing network '{network}'.")


def create_network(network):
    subprocess.run(["docker", "network", "create", network],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Created network '{network}'")


def docker_prune():
    subprocess.run(["docker", "system", "prune", "-af"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Cleaned up docker system.")


def is_container_running(container_name):
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter",
                f"name={container_name}", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        running_containers = result.stdout.strip().splitlines()
        if container_name in running_containers:
            return True
        else:
            return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking container status: {e.stderr}")
        return False


@test(10, required_files=["Dockerfile"])
def docker_build():
    # testing if the Dockerfile can be built
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"
    subprocess.check_output(
        ["docker", "build", ".", "-t", "p2"], env=environment)


@test(10, required_files=["matchdb.proto"])
def proto_compile():
    # testing if the proto file can be compiled
    os.makedirs(f"compile", exist_ok=True)
    try:
        subprocess.check_output(["python3", "-m", "grpc_tools.protoc", "-I=.", f"--python_out=compile",
                                f"--grpc_python_out=compile", "matchdb.proto"], stderr=subprocess.DEVNULL)
    except Exception as e:
        return "Error compiling matchdb.proto"


@test(5, required_files=["wins/docker-compose.yml", "Dockerfile", "server.py"])
def servers_run():
    # testing if the servers can be run
    os.chdir("wins")
    try:
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
    except Exception as e:
        return "Error running docker-compose"

    # check if the servers are running
    servers_running = False
    for _ in range(30):
        time.sleep(1)
        if is_container_running("wins-server-1"):
            if is_container_running("wins-server-2"):
                servers_running = True
                break
        print("Waiting for servers to start...")

    if not servers_running:
        return "Servers did not start in time"


def read_expected(expected_file):
    # read the expected file to an array
    # remove empty lines
    with open(expected_file, "r") as f:
        inputs = f.readlines()
        inputs = [line.strip() for line in inputs if line.strip()]
    return inputs


@test(5, required_files=["wins/docker-compose.yml", "Dockerfile", "client.py"])
def client_runs():
    # testing if client.py can be run
    # and it prints the correct number of outputs
    case_no = 0
    input_file = f"inputs/input_{case_no}.csv"

    client = subprocess.run(["docker", "run", "--net=wins_default", "p2", "python3", "/client.py", "wins-server-1:5440",
                            "wins-server-2:5440", f"/{input_file}"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    client_outputs = client.stdout.decode("utf-8").strip()
    client_outputs = client_outputs.splitlines()

    expected_file = f"outputs/expected_{case_no}.out"
    expected_outputs = read_expected(expected_file)

    if len(expected_outputs) != len(client_outputs):
        return f"Expected {len(expected_outputs)} outputs, got {len(client_outputs)}"


def test_input(case_no, full_point):
    # testing if client.py can be run
    # and it prints the correct result
    input_file = f"inputs/input_{case_no}.csv"

    client = subprocess.run(["docker", "run", "--net=wins_default", "p2", "python3", "/client.py", "wins-server-1:5440",
                            "wins-server-2:5440", f"/{input_file}"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    client_outputs = client.stdout.decode("utf-8").strip()
    client_outputs = client_outputs.splitlines()

    expected_file = f"outputs/expected_{case_no}.out"
    expected_outputs = read_expected(expected_file)

    if len(expected_outputs) != len(client_outputs):
        return f"Expected {len(expected_outputs)} outputs, got {len(client_outputs)}"

    if expected_outputs == client_outputs:
        return None
    
    print('Client outputs:', client_outputs)
    
    expected_outputs_ignore_cache = [
        line.split("*")[0] for line in expected_outputs]
    client_outputs_ignore_cache = [
        line.split("*")[0] for line in client_outputs]
    if expected_outputs_ignore_cache == client_outputs_ignore_cache:
        return TestPoint(int(full_point / 2), "Cache not working")

    return "Incorrect outputs"


@test(10)
def test_input_0():
    # test for single server
    return test_input(0, 10)


@test(20)
def test_input_1():
    return test_input(1, 20)


@test(20)
def test_input_2():
    return test_input(2, 20)


@test(20)
def test_input_3():
    return test_input(3, 20)


if __name__ == "__main__":
    tester_main()
