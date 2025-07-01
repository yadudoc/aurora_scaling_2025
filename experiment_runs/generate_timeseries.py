import os
import json
import argparse
from find_worker_timeseries import process_run_dir


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-j", "--json_input_file",
                        help="Path to a json report file")

    parser.add_argument("-r", "--run_dir",
                        help="Path to a parsl run_dir")

    parser.add_argument("-o", "--out_dir", default=".",
                        help="Path to a output_dir")

    args = parser.parse_args()

    experiments = []
    with open(args.json_input_file) as f:
        for line in f.readlines():
            experiments.append(json.loads(line))

    for e in experiments:
        output_path = f"{args.out_dir}/{e['num_nodes']}.nodes.{e['task_duration']}.s.{e['fileconfig']}.json"
        print(f"Processing output to {output_path}")
        process_run_dir(e["run_dir"], output_path)
