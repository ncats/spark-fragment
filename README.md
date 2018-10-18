Simple Spark setup to process ProbeDb
=====================================

This repository provides a simple setup to process ProbeDb with
Spark on a local cluster.

```
sbt package
```

Use the script ```submit.sh``` as an example on how to do Spark
submission. This script assumes you have the Spark distribution
installed on directory above as ```spark-2.3.1-bin-hadoop2.7```.
