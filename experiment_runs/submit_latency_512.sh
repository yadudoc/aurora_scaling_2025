#!/bin/bash -l
#PBS -l select=512
#PBS -l walltime=02:00:00
#PBS -q prod
#PBS -A workflow_scaling
#PBS -l filesystems=home:flare

cd ${PBS_O_WORKDIR}
export ZE_FLAT_DEVICE_HIERARCHY=COMPOSITE

# source $HOME/_parsl/bin/activate
source ~/setup_parsl_env.sh

ROOT=/home/yadunand/aurora_scaling_2025/experiment_runs

LOG="$ROOT/perf_latency_sleep.logs"


EXP_RECORD="$ROOT/experiment_task_efficiency_32_512.json"
EXP_CODE="$ROOT/test_latency.py"
cd $ROOT
export PYTHONPATH=$ROOT:$PYTHONPATH


for config in config_variable_workers_no_logging_prefetch
do
    echo "Running experiments with $config"
    echo "==============================================="
    for NODES in 512 256 128 64 32
    do
        for SLEEP_DUR in 10 100
        do
            for WORKERS in 12
            do
                echo "Running experiment with NODES=$NODES, SLEEP_DUR=$SLEEP_DUR, and WORKERS=$WORKERS"
                export NODES_IN_JOB=$NODES
                export MAX_WORKERS_PER_NODE=$WORKERS
                python3 $EXP_CODE --fileconfig=$config -s $SLEEP_DUR \
                        --count_per_worker=10 \
                        --nodes=$NODES \
                        --workers_per_node=$WORKERS \
                        --latency \
                        --output_record=$EXP_RECORD >> $LOG
            done
        done
    done
    echo "==============================================="
done
