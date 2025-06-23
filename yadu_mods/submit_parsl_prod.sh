#!/bin/bash -l
#PBS -l select=1024
#PBS -l walltime=02:00:00
#PBS -q prod
#PBS -A workflow_scaling
#PBS -l filesystems=home:flare

cd ${PBS_O_WORKDIR}
export ZE_FLAT_DEVICE_HIERARCHY=COMPOSITE

# source $HOME/_parsl/bin/activate
source ~/setup_parsl_env.sh

LOG="/home/yadunand/workflow_examples/parsl/high_throughput/yadu_mods/perf.logs"


EXP_RECORD="/home/yadunand/workflow_examples/parsl/high_throughput/yadu_mods/experiment_32_1024.json"
EXP_CODE="/home/yadunand/workflow_examples/parsl/high_throughput/yadu_mods/test_scale.py"
cd /home/yadunand/workflow_examples/parsl/high_throughput/yadu_mods/
export PYTHONPATH=/home/yadunand/workflow_examples/parsl/high_throughput/yadu_mods/:$PYTHONPATH


for config in config_injob_multiblock_no_logging config_injob_multiblock_prefetch
do
    echo "Running experiments with $config"
    echo "==============================================="
    for NODES in 1024
    do
        export NODES_IN_JOB=$NODES
        python3 $EXP_CODE --fileconfig=$config -s 0 --count_per_worker=100 --nodes=$NODES --output_record=$EXP_RECORD >> $LOG
        export NODES_IN_JOB=$NODES
        python3 $EXP_CODE --fileconfig=$config -s 0 --count_per_worker=10 --nodes=$NODES --output_record=$EXP_RECORD >> $LOG
    done
    echo "==============================================="
done
