import json
import re  # parsing JSON and regular expressions
import math
from collections import OrderedDict
import json
import argparse
import os
import traceback
import shutil
import multiprocessing
import subprocess

multiprocessing.set_start_method("fork")

VERBOSE = False
TEST_DIR = None

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None
DEBUG = None
GO_FOR_DEBUG = None
PROJECT_REMOTE_URL = (
    "https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/raw/main/p5/"
)


########################################################
### tester functions
########################################################


def warn(msg):
    print(f"🟡 Warning: {msg}")


def error(msg):
    print(f"🔴 Error: {msg}")


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
    return results


# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/score.json"
    print(f"Output written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


def check_for_updated_files():
    files = ["autograde.py", "p5-base.Dockerfile"]
    for file in files:
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


def tester_main():
    global VERBOSE, TEST_DIR, GO_FOR_DEBUG

    parser = argparse.ArgumentParser()
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
    parser.add_argument(
        "-k",
        "--skip-check",
        action="store_true",
        default=False,
        help="skip checking for updated files",
    )
    args = parser.parse_args()

    if not args.skip_check:
        check_for_updated_files()

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

    if CLEANUP:
        CLEANUP()

    # run init
    if INIT:
        INIT()

    # run tests
    results = run_tests()
    save_results(results)

    # run cleanup
    if CLEANUP:
        CLEANUP()


########################################################
### nbutils functions
########################################################


def parse_str_output(outputs):
    outputs = [o for o in outputs if o.get("output_type") == "execute_result"]
    if len(outputs) == 0:
        raise Exception("Output not found")
    elif len(outputs) > 1:
        raise Exception("Too many outputs")
    return "".join(outputs[0]["data"]["text/plain"]).strip()


def parse_int_output(outputs):
    return int(parse_str_output(outputs))


def parse_float_output(outputs):
    return float(parse_str_output(outputs))


def parse_bool_output(outputs):
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not bool:
        raise Exception("Error parsing output as bool")
    return eval_output


def parse_list_output(outputs):
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not list:
        raise Exception("Error parsing output as list")
    return eval_output


def parse_dict_bool_output(outputs):
    # parse outputs as a dictionary of {str: bool}
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not dict:
        raise Exception("Error parsing output as dict")

    for key in eval_output.keys():
        if type(key) is not str:
            raise Exception("Error parsing output as bool dict")
        if type(eval_output[key]) is not bool:
            raise Exception("Error parsing output as bool dict")
    return eval_output


def parse_dict_float_output(outputs):
    # parse outputs as a dictionary of {str: float}
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not dict:
        raise Exception("Error parsing output as dict")

    for key in eval_output.keys():
        if type(key) is not str:
            raise Exception("Error parsing output as float dict")
        if type(eval_output[key]) is not float:
            raise Exception("Error parsing output as float dict")
    return eval_output


def parse_dict_int_output(outputs):
    # parse outputs as a dictionary of {str: int}
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not dict:
        raise Exception("Error parsing output as dict")

    for key in eval_output.keys():
        if type(key) is not str:
            raise Exception("Error parsing output as int dict")
        if type(eval_output[key]) is not int:
            raise Exception("Error parsing output as int dict")
    return eval_output


def is_accurate(lower, actual):
    if math.isnan(lower) and math.isnan(actual):
        return True
    return lower <= actual


def compare_bool(expected, actual):
    return expected == actual


def compare_int(expected, actual):
    return expected == actual


def compare_type(expected, actual):
    return expected == actual


def compare_float(expected, actual, tolerance=0.01):
    if math.isnan(expected) and math.isnan(actual):
        return True
    return math.isclose(expected, actual, rel_tol=tolerance)


def compare_str(expected, actual, case_sensitive=True):
    if not case_sensitive:
        return expected.upper() == actual.upper()
    return expected == actual


def compare_list(expected, actual, strict_order=True):
    if strict_order:
        return expected == actual
    else:
        return sorted(expected) == sorted(actual)


def compare_tuple(expected, actual):
    return expected == actual


def compare_set(expected, actual, superset=False):
    if superset:
        return len(expected - actual) == 0
    else:
        return expected == actual


def compare_dict(expected, actual, tolerance=0.01):
    if tolerance:
        if expected.keys() != actual.keys():
            return False

        for key in expected.keys():
            if not compare_float(expected[key], actual[key], tolerance):
                return False

        return True

    return expected == actual


def compare_dict_floats(expected, actual, tolerance=0.01):
    if tolerance:
        if expected.keys() != actual.keys():
            return False

        for key in expected.keys():
            if not compare_float(expected[key], actual[key], tolerance):
                return False

        return True

    return expected == actual


def compare_dict_bools(expected, actual):
    if expected.keys() != actual.keys():
        return False

    for key in expected.keys():
        if not compare_bool(expected[key], actual[key]):
            return False

    return True


def compare_dict_ints(expected, actual):
    if expected.keys() != actual.keys():
        return False

    for key in expected.keys():
        if not compare_int(expected[key], actual[key]):
            return False

    return True


########################################################
### autograder functions
########################################################

ANSWERS = (
    {}
)  # global variable to store answers { key = question number, value = output of the answer cell }


@init
def collect_cells():
    with open("nb/p5.ipynb") as f:
        nb = json.load(f)  # load the notebook as a json object
        cells = nb["cells"]  # get the list of cells from the notebook
        expected_exec_count = 1  # expected execution count of the next cell

        for cell in cells:
            if "execution_count" in cell and cell["execution_count"]:
                exec_count = cell["execution_count"]
                if exec_count != expected_exec_count:
                    raise Exception(
                        f"Expected execution count {expected_exec_count} but found {exec_count}. Please do Restart & Run all then save before running the tester."
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

            # found a answer cell, add its output to list
            qnum = int(m.group(1))
            notes = m.group(2).strip()
            if qnum in ANSWERS:
                raise Exception(f"Answer {qnum} repeated!")
            # expected qnum = 1 + (max key in ANSWERS dictionary if ANSWERS is not empty else 0)
            expected = 1 + (max(ANSWERS.keys()) if ANSWERS else 0)
            if qnum != expected:
                print(f"Warning: Expected question {expected} next but found {qnum}!")

            # add the output of the answer cell to the ANSWERS dictionary
            ANSWERS[qnum] = cell["outputs"]


@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    outputs = ANSWERS[1]
    output = parse_int_output(outputs)
    if not compare_int(298, output):
        return "Wrong answer"


@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    outputs = ANSWERS[2]
    output = parse_int_output(outputs)
    if not compare_int(298, output):
        return "Wrong answer"


@test(points=10)
def q3():
    if not 3 in ANSWERS:
        raise Exception("Answer to question 3 not found")
    outputs = ANSWERS[3]
    output = parse_int_output(outputs)
    if not compare_int(298, output):
        return "Wrong answer"


# desugars to test(points=10)(q1) = wrapper(q1) -> TESTS["q1"] = _unit_test(q1, 10, None, "")
@test(points=10)
def q4():
    if not 4 in ANSWERS:
        raise Exception("Answer to question 4 not found")
    outputs = ANSWERS[4]
    output = parse_dict_bool_output(outputs)

    if not compare_dict_bools(
        {
            "banks": False,
            "loans": False,
            "action_taken": True,
            "counties": True,
            "denial_reason": True,
            "ethnicity": True,
            "loan_purpose": True,
            "loan_type": True,
            "preapproval": True,
            "property_type": True,
            "race": True,
            "sex": True,
            "states": True,
            "tracts": True,
        },
        output,
    ):
        return "Wrong answer"


@test(points=10)
def q5():
    if not 5 in ANSWERS:
        raise Exception("Answer to question 5 not found")
    outputs = ANSWERS[5]
    # print("test 5 outputs: ", outputs)
    output = parse_int_output(outputs)
    if not compare_int(19739, output):
        return "Wrong answer"


@test(points=10)
def q6():
    if not 6 in ANSWERS:
        raise Exception("Answer to question 6 not found")
    # to be manually graded


@test(points=10)
def q7():
    if not 7 in ANSWERS:
        raise Exception("Answer to question 7 not found")
    outputs = ANSWERS[7]
    output = parse_dict_float_output(outputs)

    if not compare_dict_floats(
        {
            "Milwaukee": 3.1173465727097907,
            "Waukesha": 2.8758225602027756,
            "Washington": 2.851009389671362,
            "Dane": 2.890674955595027,
            "Brown": 3.010949119373777,
            "Racine": 3.099783715012723,
            "Outagamie": 2.979661835748792,
            "Winnebago": 3.0284761904761908,
            "Ozaukee": 2.8673765432098772,
            "Sheboygan": 2.995511111111111,
        },
        output,
        tolerance=1e-5,
    ):
        return "Wrong answer"


@test(points=10)
def q8():
    if not 8 in ANSWERS:
        raise Exception("Answer to question 8 not found")
    # to be manually graded


@test(points=10)
def q9():
    if not 9 in ANSWERS:
        raise Exception("Answer to question 9 not found")
    outputs = ANSWERS[9]
    output = parse_dict_float_output(outputs)

    if not compare_dict_floats(
        {
            "depth=1": 0.8809425750509244,
            "depth=5": 0.8929195560947918,
            "depth=10": 0.8954796914480349,
            "depth=15": 0.8948674851679115,
            "depth=20": 0.893943610236089,
        },
        output,
        tolerance=1e-4,
    ):
        return "Wrong answer"


@test(points=10)
def q10():
    if not 10 in ANSWERS:
        raise Exception("Answer to question 10 not found")
    # to be manually graded


if __name__ == "__main__":
    tester_main()
