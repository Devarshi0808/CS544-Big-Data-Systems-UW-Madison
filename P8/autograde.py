import ast
import re
from collections import OrderedDict
import json
import os
import traceback
import shutil
import subprocess
import argparse
import math

import multiprocessing

multiprocessing.set_start_method("fork")


def warn(msg):
    print(f"🟡 Warning: {msg}")


def error(msg):
    print(f"🔴 Error: {msg}")


def info(msg):
    print(f"🔵 Info: {msg}")


ARGS = None
DEBUG = False
VERBOSE = False
TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None
DEBUG_DIR = "_autograder_results"
PROJECT_REMOTE_URL = (
    "https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/tree/main/p8"
)

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None


def verbose(msg):
    if VERBOSE:
        print(msg)


def run_with_timeout(func, timeout):
    def wrapper(ret):
        try:
            func()
            result = None
        except Exception as e:
            result = traceback.format_exception(e)
        ret.send(result)

    ret_send, ret_recv = multiprocessing.Pipe()
    proc = multiprocessing.Process(target=wrapper, args=(ret_send,))
    proc.start()
    proc.join(timeout)
    if proc.is_alive():
        proc.terminate()
        result = "Timeout"
    else:
        result = ret_recv.recv()
    return result


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
            else:
                print(f"Test {self.func.__name__} failed:\n")
                if isinstance(result, str):
                    result = result.split("\n")
                if isinstance(result, list):
                    print("\n".join(result) + "\n")
                else:
                    print(result + "\n")
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
        TESTS[test_func.__name__] = _unit_test(test_func, points, timeout, desc)

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


# get arguments
def get_args():
    return ARGS


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

    if DEBUG:
        debug_abs_path = f"{TEST_DIR}/{DEBUG_DIR}"
        shutil.rmtree(debug_abs_path, ignore_errors=True)
        shutil.copytree(src=TMP_DIR, dst=debug_abs_path, dirs_exist_ok=True)
        print(f"Run results are stored to {debug_abs_path}")

    return results


# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/score.json"
    print(f"Output written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


def check_files(test_dir, required_files):
    if not os.path.isdir(f"{test_dir}/.git"):
        warn(f"{test_dir} is not a repository")

    missing_files = []
    for file in required_files:
        if not os.path.exists(f"{test_dir}/{file}"):
            missing_files.append(file)
    if len(missing_files) > 0:
        msg = ", ".join(missing_files)
        warn(f"the following required files are missing: {msg}")


def check_for_updated_files():
    files = ["autograde.py"]
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


def tester_main(parser, required_files=None):
    if required_files is None:
        required_files = []

    global ARGS, VERBOSE, TEST_DIR, DEBUG

    parser.add_argument(
        "-d", "--dir", type=str, default=".", help="path to your repository"
    )
    parser.add_argument("-l", "--list", action="store_true", help="list all tests")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-g",
        "--debug",
        action="store_true",
        help="create a debug directory with the files used while testing",
    )
    args = parser.parse_args()

    ARGS = args

    if args.list:
        list_tests()
        return

    VERBOSE = args.verbose
    DEBUG = args.debug
    test_dir = args.dir
    if not os.path.isdir(test_dir):
        error("invalid path")
        return
    TEST_DIR = os.path.abspath(test_dir)

    # check if required files are present
    check_files(test_dir, required_files)

    # clean TMP directory
    shutil.rmtree(TMP_DIR, ignore_errors=True)

    # make a copy of the code
    def ignore(_dir_name, _dir_content):
        return [".git", ".github", "__pycache__", ".gitignore", "*.pyc", DEBUG_DIR]

    shutil.copytree(src=TEST_DIR, dst=TMP_DIR, dirs_exist_ok=True, ignore=ignore)
    os.chdir(TMP_DIR)

    # run init
    if INIT:
        ret = INIT()
        if ret != None:
            result = f"Init failed: {ret}"
            error(result)
            save_results(result)
            exit(-1)

    # run tests
    results = run_tests()
    save_results(results)

    # run cleanup
    if CLEANUP:
        print("Cleaning...")
        ret = CLEANUP()
        if ret != None:
            result = f"Cleanup failed: {ret}"
            error(result)
            exit(-1)


# ============= end of legacy code =============

# ============= start of autograde.py =============


ANSWERS = {}  # global variable to store answers { key = question number, value = output of the answer cell }

FILE_NOT_FOUND = False


class NBUtils:
    @classmethod
    def parse_str_output(cls, outputs):
        outputs = [o for o in outputs if o.get("output_type") == "execute_result"]
        if len(outputs) == 0:
            raise Exception("Output not found")
        elif len(outputs) > 1:
            raise Exception("Too many outputs")
        return "".join(outputs[0]["data"]["text/plain"]).strip()

    @classmethod
    def parse_int_output(cls, outputs):
        return int(cls.parse_str_output(outputs))

    @classmethod
    def parse_float_output(cls, outputs):
        return float(cls.parse_str_output(outputs))

    @classmethod
    def parse_bool_output(cls, outputs):
        eval_output = eval(cls.parse_str_output(outputs))
        if type(eval_output) is not bool:
            raise Exception("Error parsing output as bool")
        return eval_output

    @classmethod
    def parse_list_output(cls, outputs):
        eval_output = eval(cls.parse_str_output(outputs))
        if type(eval_output) is not list:
            raise Exception("Error parsing output as list")
        return eval_output

    @classmethod
    def parse_dict_bool_output(cls, outputs):
        # parse outputs as a dictionary of {str: bool}
        eval_output = eval(cls.parse_str_output(outputs))
        if type(eval_output) is not dict:
            raise Exception("Error parsing output as dict")

        for key in eval_output.keys():
            if type(key) is not str:
                raise Exception("Error parsing output as bool dict")
            if type(eval_output[key]) is not bool:
                raise Exception("Error parsing output as bool dict")
        return eval_output

    @classmethod
    def parse_dict_float_output(cls, outputs):
        # parse outputs as a dictionary of {str: float}
        eval_output = eval(cls.parse_str_output(outputs))
        if type(eval_output) is not dict:
            raise Exception("Error parsing output as dict")

        for key in eval_output.keys():
            if type(key) is not str:
                raise Exception("Error parsing output as float dict")
            if type(eval_output[key]) is not float:
                raise Exception("Error parsing output as float dict")
        return eval_output

    @classmethod
    def parse_dict_int_output(cls, outputs):
        # parse outputs as a dictionary of {str: int}
        eval_output = eval(cls.parse_str_output(outputs))
        if type(eval_output) is not dict:
            raise Exception("Error parsing output as dict")

        for key in eval_output.keys():
            if type(key) is not str:
                raise Exception("Error parsing output as int dict")
            if type(eval_output[key]) is not int:
                raise Exception("Error parsing output as int dict")
        return eval_output

    @classmethod
    def is_accurate(cls, lower, actual):
        if math.isnan(lower) and math.isnan(actual):
            return True
        return lower <= actual

    @classmethod
    def compare_bool(cls, expected, actual):
        return expected == actual

    @classmethod
    def compare_int(cls, expected, actual):
        return expected == actual

    @classmethod
    def compare_type(cls, expected, actual):
        return expected == actual

    @classmethod
    def compare_float(cls, expected, actual, tolerance=0.01):
        if math.isnan(expected) and math.isnan(actual):
            return True
        return math.isclose(expected, actual, rel_tol=tolerance)

    @classmethod
    def compare_str(cls, expected, actual, case_sensitive=True):
        if not case_sensitive:
            return expected.upper() == actual.upper()
        return expected == actual

    @classmethod
    def compare_list(cls, expected, actual, strict_order=True):
        if strict_order:
            return expected == actual
        else:
            return sorted(expected) == sorted(actual)

    @classmethod
    def compare_tuple(cls, expected, actual):
        return expected == actual

    @classmethod
    def compare_set(cls, expected, actual, superset=False):
        if superset:
            return len(expected - actual) == 0
        else:
            return expected == actual

    @classmethod
    def compare_dict(cls, expected, actual, tolerance=0.01):
        if tolerance:
            if expected.keys() != actual.keys():
                return False

            for key in expected.keys():
                if not cls.compare_float(expected[key], actual[key], tolerance):
                    return False

            return True

        return expected == actual

    @classmethod
    def compare_dict_floats(cls, expected, actual, tolerance=0.01):
        if tolerance:
            if expected.keys() != actual.keys():
                return False

            for key in expected.keys():
                if not cls.compare_float(expected[key], actual[key], tolerance):
                    return False

            return True

        return expected == actual

    @classmethod
    def compare_dict_bools(cls, expected, actual):
        if expected.keys() != actual.keys():
            return False

        for key in expected.keys():
            if not cls.compare_bool(expected[key], actual[key]):
                return False

        return True

    @classmethod
    def compare_dict_ints(cls, expected, actual):
        if expected.keys() != actual.keys():
            return False

        for key in expected.keys():
            if not cls.compare_int(expected[key], actual[key]):
                return False

        return True

    @classmethod
    def compare_figure(cls, expected, actual):
        return type(expected) == type(actual)


@init
def collect_cells(*args, **kwargs):
    global FILE_NOT_FOUND
    if not os.path.exists("src/p8.ipynb"):
        FILE_NOT_FOUND = True
        return

    with open("src/p8.ipynb") as f:
        nb = json.load(f)  # load the notebook as a json object
        cells = nb["cells"]  # get the list of cells from the notebook
        expected_exec_count = 1  # expected execution count of the next cell

        for cell in cells:
            if "execution_count" in cell and cell["execution_count"]:
                exec_count = cell["execution_count"]
                if exec_count != expected_exec_count:
                    raise Exception(
                        f"""
                        Expected execution count {expected_exec_count} but found {exec_count}. 
                        Please do Restart & Run all then save before running the tester.
                        """
                    )
                expected_exec_count = exec_count + 1

            if cell["cell_type"] != "code":
                continue

            if not cell["source"]:
                # if the cell is empty, skip it (empty = it has no source code)
                continue

            # pattern should be #q1 or #Q1 (#q2 or #Q2, etc.)
            m = re.match(r"#[qQ](\d+)(.*)", cell["source"][0].strip())
            if not m:
                continue

            qnum = int(m.group(1))
            if qnum in ANSWERS:
                raise Exception(f"Answer {qnum} repeated!")
            expected = 1 + (max(ANSWERS.keys()) if ANSWERS else 0)
            if qnum != expected:
                print(f"Warning: Expected question {expected} next but found {qnum}!")
            ANSWERS[qnum] = cell["outputs"]


@test(points=10)
def q1():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 1 not in ANSWERS:
        return "ERROR: Answer to question 1 not found"
    outputs = ANSWERS[1]
    output = int(NBUtils.parse_str_output(outputs).replace("'", ""))
    if output != 55025:
        return "Wrong answer"


@test(points=10)
def q2():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 2 not in ANSWERS:
        return "ERROR: Answer to question 2 not found"

    output = NBUtils.parse_dict_int_output(ANSWERS[2])
    if not NBUtils.compare_dict_ints(
        {"48": 254, "13": 159, "51": 133, "21": 120, "29": 115}, output
    ):
        return "Wrong answer"


@test(points=10)
def q3():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 3 not in ANSWERS:
        return "ERROR: Answer to question 3 not found"
    output = ast.literal_eval(NBUtils.parse_str_output(ANSWERS[3]))
    assert output == {"q1": "10 MB", "q2": "10 MB"}, "Wrong answer"


@test(points=10)
def q4():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 4 not in ANSWERS:
        return "ERROR: Answer to question 4 not found"
    output = NBUtils.parse_list_output(ANSWERS[4])
    if "p8" not in output:
        return "Wrong answer"


@test(points=10)
def q5():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 5 not in ANSWERS:
        return "ERROR: Answer to question 5 not found"
    output = NBUtils.parse_dict_int_output(ANSWERS[5])
    if not NBUtils.compare_dict_ints(
        {
            "Milwaukee": 46570,
            "Dane": 38557,
            "Waukesha": 34159,
            "Brown": 15615,
            "Racine": 13007,
            "Outagamie": 11523,
            "Kenosha": 10744,
            "Washington": 10726,
            "Rock": 9834,
            "Winnebago": 9310,
        },
        output,
    ):
        return "Wrong answer"


@test(points=10)
def q6():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 6 not in ANSWERS:
        return "ERROR: Answer to question 6 not found"
    output = NBUtils.parse_int_output(ANSWERS[6])
    if output < 1:
        return "Wrong answer. There should be at least 1 application with your chosen income"


@test(points=10)
def q7():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 7 not in ANSWERS:
        return "ERROR: Answer to question 7 not found"
    output = NBUtils.parse_float_output(ANSWERS[7])
    if output < 0 or output > 100:
        return "Invalid R^2."



@test(points=10)
def q8():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 8 not in ANSWERS:
        return "ERROR: Answer to question 8 not found"
    try:
        NBUtils.parse_float_output(ANSWERS[8])  # check if answer is a valid float
        return
    except Exception as e:
        return f"Invalid format. Expected a float: {e}"


@test(points=10)
def q9():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 9 not in ANSWERS:
        return "ERROR: Answer to question 9 not found"
    output = NBUtils.parse_dict_int_output(ANSWERS[9])
    counties = [
        "Bayfield",
        "Door",
        "Jackson",
        "Richland",
        "Burnett",
        "Vernon",
        "Iron",
        "Trempealeau",
        "Waupaca",
        "Pepin",
        "Waushara",
        "Polk",
        "Washburn",
        "Buffalo",
        "Vilas",
        "Oneida",
        "Taylor",
        "Marquette",
        "Juneau",
        "Lafayette",
        "Sawyer",
        "Ashland",
        "Langlade",
        "Adams",
        "Crawford",
        "Barron",
        "Monroe",
        "Price",
        "Forest",
        "Green Lake",
        "Clark",
        "Rusk",
        "Outagamie",
        "Calumet",
        "Sauk",
        "Dodge",
        "Kenosha",
        "Douglas",
        "Chippewa",
        "Eau Claire",
        "Fond du Lac",
        "Brown",
        "Kewaunee",
        "Oconto",
        "Florence",
        "Rock",
        "La Crosse",
        "Iowa",
        "Dane",
        "Columbia",
        "Green",
        "Manitowoc",
        "Marinette",
        "Dunn",
        "Milwaukee",
        "Waukesha",
        "Washington",
        "Ozaukee",
        "Pierce",
        "St. Croix",
        "Winnebago",
        "Grant",
        "Racine",
        "Shawano",
        "Menominee",
        "Sheboygan",
        "Portage",
        "Jefferson",
        "Marathon",
        "Lincoln",
        "Walworth",
        "Wood",
    ]
    for county,  count in output.items():
        if county not in counties:
            return f"'{county}' is not a valid county in Wisconsin."
        if count < 1:
            return f"Invalid application count for {county}: got {count}."

@test(points=10)
def q10():
    if FILE_NOT_FOUND:
        return "ERROR: File src/p8.ipynb not found"
    if 10 not in ANSWERS:
        return "ERROR: Answer to question 10 not found"
    output = set(NBUtils.parse_list_output(ANSWERS[10]))
    if output != {'Columbia', 'Dodge', 'Green', 'Iowa', 'Jefferson', 'Rock', 'Sauk'}:
        return "Wrong answer."


if __name__ == "__main__":
    tester_main(
        parser=argparse.ArgumentParser(),
        required_files=["src/p8.ipynb"],
    )
