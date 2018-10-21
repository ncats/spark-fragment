Simple Spark setup to process fragments
=======================================

This repository provides a simple setup to process fragments with
Spark on a local standalone cluster.

```
sbt package
sbt assemblyPackageDependency
```

Use the script ```submit.sh``` as an example on how to do Spark
submission. This script assumes you have the Spark distribution
installed one directory above as ```spark-2.3.1-bin-hadoop2.7```.
