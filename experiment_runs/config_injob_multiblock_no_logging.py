import os
from parsl.config import Config

# This is just one example config, please see the Aurora documentation for parsl for more
# Config versions and options: https://docs.alcf.anl.gov/aurora/workflows/parsl/

# Use LocalProvider to launch workers within a submitted batch job
from parsl.providers import LocalProvider
# The high throughput executor is for scaling large single core/tile/gpu tasks on HPC system:
from parsl.executors import HighThroughputExecutor
# Use the MPI launcher to launch worker processes:
from parsl.launchers import MpiExecLauncher

tile_names = [f'{gid}.{tid}' for gid in range(6) for tid in range(2)]
start_threads = [1,9,17,25,33,41,53,61,69,77,85,93]
threads_by_tile = [f"{st}-{st+7},{st+104}-{st+111}" for st in start_threads]
cpu_affinity = "list:"+":".join(threads_by_tile)

# Get the number of nodes:
node_file = os.getenv("PBS_NODEFILE")
with open(node_file,"r") as f:
    node_list = f.readlines()
    num_nodes = len(node_list)

nodes_per_block = min(num_nodes, 512)
max_blocks = max(num_nodes // nodes_per_block, 1)

nodes_per_block = os.environ.get("NODES_IN_JOB", nodes_per_block)

print(f"Running using {nodes_per_block} nodes")

MAX_WORKERS_PER_NODE=int(os.environ.get("MAX_WORKERS_PER_NODE", 12))

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=MAX_WORKERS_PER_NODE,
            # Distributes threads to workers/tiles in a way optimized for Aurora
            cpu_affinity=cpu_affinity,
            # Increase if you have many more tasks than workers
            prefetch_capacity=0,
            # Options that specify properties of PBS Jobs
            provider=LocalProvider(
                # Number of nodes job
                nodes_per_block=nodes_per_block,
                launcher=MpiExecLauncher(bind_cmd="--cpu-bind", overrides="--ppn 1"),
                init_blocks=max_blocks,
                max_blocks=max_blocks,
            ),
        ),
    ],
    initialize_logging=False
)
