#!/bin/bash -l
#PBS -l select=16
#PBS -l walltime=01:00:00
#PBS -q debug-scaling
#PBS -A workflow_scaling
#PBS -l filesystems=home:flare

cd ${PBS_O_WORKDIR}
export ZE_FLAT_DEVICE_HIERARCHY=COMPOSITE

# source $HOME/_parsl/bin/activate
module load frameworks
source /home/yadunand/aurora_scaling_2025/experiment_runs/kevin_single_socket_parsl/bin/activate

ROOT=/home/yadunand/aurora_scaling_2025/experiment_runs

LOG="$ROOT/perf_1k_sleep.logs"


EXP_RECORD="$ROOT/experiment_single_socket_16_100x.json"
EXP_CODE="$ROOT/test_scale.py"
cd $ROOT
export PYTHONPATH=$ROOT:$PYTHONPATH


for config in config_variable_workers_no_logging_prefetch
do
    echo "Running experiments with $config"
    echo "==============================================="
    for NODES in 16 8 4 2 1
    do
        for SLEEP_DUR in 0
        do
            for WORKERS in 12 64
            do
                echo "Running experiment with NODES=$NODES, SLEEP_DUR=$SLEEP_DUR, and WORKERS=$WORKERS"
                export NODES_IN_JOB=$NODES
                export MAX_WORKERS_PER_NODE=$WORKERS
                python3 $EXP_CODE --fileconfig=$config -s $SLEEP_DUR \
                        --count_per_worker=100 \
                        --nodes=$NODES \
                        --workers_per_node=$WORKERS \
                        --output_record=$EXP_RECORD >> $LOG
            done
        done
    done
    echo "==============================================="
done
