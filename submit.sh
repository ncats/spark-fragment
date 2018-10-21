#!/bin/sh

# make sure to set SPARK_WORKER_INSTANCES before launching start_slave.sh
jars="lib/ojdbc8.jar,lib/lychi-all-fe2ea2a.jar,lib/tripod_2.11-play_2_6-20181017-37d3106.jar"
cmd="../spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
	--jars $jars \
	--master spark://`hostname`:7077 \
	--executor-memory 8G \
	--total-executor-cores 24 \
	--class probedb.SparkProbeDb target/scala-2.11/spark-probedb_2.11-0.0.1.jar"
exec="$cmd $*" 
echo $exec
exec $exec
