import argparse
import os
import glob
import json
from datetime import datetime
from datetime import timedelta


def get_tstamp(logline):
    # return datetime.strptime(logline[0:23], '%Y-%m-%d %H:%M:%S.%f')
    return logline[0:23]


def parse_worker_logs(worker_log_path: str):

    with open(worker_log_path) as f:
        loglines = f.readlines()

    task_arrival_gaps = []

    task_completed_time = 0
    first_task_arrival = None
    start_time = None


    first_t = get_tstamp(loglines[0])
    last_t = get_tstamp(loglines[-1])


    worker_log = []

    worker_log.append({"timestamp": first_t,
                       "utilization": 0})

    for logline in loglines:
        tstamp = get_tstamp(logline)
        if "Received" in logline:
            worker_log.append({"timestamp": tstamp,
                               "utilization": 1})
        elif "All processing" in logline:
            worker_log.append({"timestamp": tstamp,
                               "utilization": -1})

    return worker_log


def process_run_dir(run_dir, output_file):

    block_path = f"{run_dir}/HighThroughputExecutor/block-0/"
    base = os.path.basename(run_dir)
    print(f"Processing {block_path}")

    manager_dirs = glob.glob(f"{block_path}/*")

    print(manager_dirs)
    worker_records = []
    manager_records = []
    for manager_dir in manager_dirs:
        manager_id = os.path.basename(manager_dir)
        print(f"{manager_id}")
        records = []
        for worker_log in glob.glob(f"{manager_dir}/worker*log"):
            print(f"Loading {worker_log}")
            worker_record = parse_worker_logs(worker_log)

            worker_records.extend(worker_record)

    with open(output_file, "a") as f:
        for w in worker_records:
            f.write(json.dumps(w) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-w", "--worker_file",
                        help="Path to a worker log file")

    parser.add_argument("-r", "--run_dir",
                        help="Path to a parsl run_dir")

    parser.add_argument("--wrk_json",
                        help="Path to a output file")

    args = parser.parse_args()

    if args.worker_file:
        record = parse_worker_logs(args.worker_file)
        print(record)

    elif args.run_dir:

        assert args.wrk_json, "--wrk_json required to write outputs to"

        process_run_dir(args.run_dir, args.wrk_json)
