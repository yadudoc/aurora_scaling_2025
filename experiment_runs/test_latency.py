#!/usr/bin/env python3

import argparse
import time
import json
from collections import OrderedDict

import parsl
from parsl.app.app import python_app  # , bash_app
import concurrent.futures


@python_app
def double(x):
    return x * 2


@python_app
def echo(x, string, stdout=None):
    print(string)
    return x * 5


@python_app
def import_echo(x, string, stdout=None):
    # from time import sleep
    # sleep(0)
    print(string)
    return x * 5


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


def test_simple(n=2):
    start = time.time()
    x = double(n)
    print("Result : ", x.result())
    assert x.result() == n * \
        2, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_imports(n=2):
    start = time.time()
    x = import_echo(n, "hello world")
    print("Result : ", x.result())
    assert x.result() == n * \
        5, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_platform(n=2, sleep=1):

    dfk = parsl.dfk()
    # sync
    x = platform(sleep=0)
    print(x.result())

    name = list(dfk.executors.keys())[0]
    print("Trying to get executor : ", name)

    print("Executor : ", dfk.executors[name])
    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)
    d = []
    for i in range(0, n):
        x = platform(sleep=sleep)
        d.append(x)

    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    print(set([i.result()for i in d]))

    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    return True


def test_parallel_for(num_tasks_per_worker=64, num_nodes=1, sleep=1, workers_per_node=None):
    d = {}

    start = time.time()
    print("Priming ...")
    double(10).result()
    delta = time.time() - start
    print("Priming done in {} seconds".format(delta))


    total_tasks = num_nodes * workers_per_node * num_tasks_per_worker

    start = time.time()

    for i in range(0, total_tasks):
        d[i] = platform(sleep=sleep)

    launch_time = time.time() - start
    [d[i].result() for i in d]
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(total_tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(total_tasks / delta))
    record = {"throughput": total_tasks / delta,
              "total_tasks": total_tasks,
              "num_nodes": num_nodes,
              "workers_per_node": workers_per_node,
              "tasks_per_worker": num_tasks_per_worker,
              "total_time_to_launch": launch_time,
              "total_time_to_complete": delta}

    return record


def test_parallel_with_task_latency(num_tasks_per_worker=64, num_nodes=1, sleep=1, workers_per_node=None):
    future_table = OrderedDict()
    start = time.time()
    print("Priming ...")
    double(10).result()
    delta = time.time() - start
    print("Priming done in {} seconds".format(delta))


    total_tasks = num_nodes * workers_per_node * num_tasks_per_worker

    start = time.time()

    for i in range(0, total_tasks):
        future = platform(sleep=sleep)
        future_table[future] = time.perf_counter()

    launch_time = time.time() - start

    for future in concurrent.futures.as_completed(future_table):
        future_table[future] = round(time.perf_counter() - future_table[future], 3)

    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(total_tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(total_tasks / delta))

    # Bin the per-task latencies into 100 batches

    record = {"throughput": total_tasks / delta,
              "total_tasks": total_tasks,
              "num_nodes": num_nodes,
              "workers_per_node": workers_per_node,
              "tasks_per_worker": num_tasks_per_worker,
              "total_time_to_launch": launch_time,
              "total_time_to_complete": delta,
              "latencies": list(future_table.values()),
              }

    return record


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sleep", default="0")
    parser.add_argument("-c", "--count_per_worker", default="10",
                        help="Count of apps to launch per node")
    parser.add_argument("-n", "--nodes", default="10",
                        help="Nodes in this experiment config")
    parser.add_argument("-w", "--workers_per_node", default="64",
                        help="Workers per node")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-l", "--latency", action='store_true',
                        help="Report individual task latencies")
    parser.add_argument("-f", "--fileconfig", required=True)
    parser.add_argument("-o", "--output_record", required=True)

    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    config = None
    exec("from {} import config".format(args.fileconfig))
    with parsl.load(config) as dfk:
        # x = test_simple(int(args.count))
        # x = test_imports()

        if args.latency:
            print("Measuring task latencies")
            record = test_parallel_with_task_latency(
                int(args.count_per_worker),
                int(args.nodes),
                float(args.sleep),
                workers_per_node=int(args.workers_per_node)
            )

        else:
            record = test_parallel_for(int(args.count_per_worker), int(args.nodes),
                                       float(args.sleep),
                                       workers_per_node=int(args.workers_per_node))

        record["fileconfig"] = args.fileconfig
        record["task_duration"] = args.sleep
        record["run_dir"] = dfk.run_dir

        exp_record = json.dumps(record)

        print(exp_record)
        with open(args.output_record, "a") as f:
            f.write(exp_record + "\n")

