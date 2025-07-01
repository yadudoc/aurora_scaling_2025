import argparse
import os
import glob
import json
from datetime import datetime


def parse_worker_logs(worker_log_path: str):

    with open(worker_log_path) as f:
        loglines = f.readlines()

    task_arrival_gaps = []

    task_completed_time = 0
    first_task_arrival = None
    start_time = None
    for logline in loglines:
        tstamp = datetime.strptime(logline[0:23], '%Y-%m-%d %H:%M:%S.%f')
        end_time = tstamp
        if not start_time:
            start_time = tstamp

        if "Received" in logline:
            if not first_task_arrival:
                first_task_arrival = tstamp
            if task_completed_time:
                task_arrival_gap = (tstamp - task_completed_time).total_seconds()
            else:
                task_arrival_gap = 0

            task_arrival_gaps.append(task_arrival_gap)

        elif "All processing" in logline:
            task_completed_time = tstamp

    # Skip the first intertask-arrival time to avoid priming task
    task_arrival_gaps = task_arrival_gaps[1:]

    if task_arrival_gaps:
        avg = sum(task_arrival_gaps) / len(task_arrival_gaps)
        record = {
            "minimum_gap": min(task_arrival_gaps),
            "maximum_gap": max(task_arrival_gaps),
            "average_gap": avg,
            "total_tasks": len(task_arrival_gaps),
            "worker_start_time": str(start_time),
            "worker_end_time": str(end_time),
            "first_task_arrival": str(first_task_arrival),
        }
    else:
        record = {
            "minimum_gap": 0,
            "maximum_gap": 0,
            "average_gap": 0,
            "total_tasks": len(task_arrival_gaps),
            "worker_start_time": str(start_time),
            "worker_end_time": str(end_time),
            "first_task_arrival": 0,
        }
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-w", "--worker_file",
                        help="Path to a worker log file")

    parser.add_argument("-r", "--run_dir",
                        help="Path to a parsl run_dir")

    parser.add_argument("--mgr_json",
                        help="Path to a output file")
    parser.add_argument("--wrk_json",
                        help="Path to a output file")

    args = parser.parse_args()

    if args.worker_file:
        record = parse_worker_logs(args.worker_file)
        print(record)

    elif args.run_dir:

        assert args.mgr_json, "--mgr_json required to write outputs to"
        assert args.wrk_json, "--wrk_json required to write outputs to"

        block_path = f"{args.run_dir}/HighThroughputExecutor/block-0/"
        base = os.path.basename(args.run_dir)
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
                records.append(worker_record)

            total_tasks = sum([r["total_tasks"] for r in records])
            if total_tasks:
                avg_gap = sum([r["average_gap"] * r["total_tasks"] for r in records]) / total_tasks
            else:
                avg_gap = 0
            manager_record = {"manager_id": manager_id,
                              "total_tasks": total_tasks,
                              "minimum_gap": min([r["minimum_gap"] for r in records]),
                              "maximum_gap": min([r["maximum_gap"] for r in records]),
                              "avg_gap": avg_gap,
                              }
            worker_records.extend(records)
            manager_records.append(manager_record)

        with open(args.wrk_json, "a") as f:
            for w in worker_records:
                f.write(json.dumps(w) + "\n")

        with open(args.mgr_json, "a") as f:
            for w in manager_records:
                f.write(json.dumps(w) + "\n")
