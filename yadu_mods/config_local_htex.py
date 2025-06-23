import os
from parsl.config import Config

from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=64,
            # Increase if you have many more tasks than workers
            prefetch_capacity=128,
            # Options that specify properties of PBS Jobs
            provider=LocalProvider(
                # Number of nodes job
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
            ),
        ),
    ],
    initialize_logging=False,
)
