#!/bin/sh

libs="target/scala-2.11/spark-fragments-assembly-0.0.1-deps.jar"
jar="target/scala-2.11/spark-fragments_2.11-0.0.1.jar"
# make sure to set SPARK_WORKER_INSTANCES before launching start_slave.sh
cmd="../spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
    --jars $libs
    --master spark://`hostname`:7077 \
    --executor-memory 4G \
    --total-executor-cores 24 \
    --class ncats.SparkFragments $jar"
exec="$cmd $*" 
echo $exec
exec $exec
