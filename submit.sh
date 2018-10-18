#!/bin/sh
cmd="../spark-2.3.1-bin-hadoop2.7/bin/spark-submit --jars lib/ojdbc8.jar,lib/lychi-all-fe2ea2a.jar,lib/tripod_2.11-play_2_6-20181017-37d3106.jar --class probedb.SparkProbeDb target/scala-2.11/spark-probedb_2.11-0.0.1.jar $*"
echo $cmd
exec $cmd
