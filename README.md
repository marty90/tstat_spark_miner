# Tstat-Spark-Miner
This tool allows to perform simple queries to a Spark based cluster where Tstat log files are stored.

## 1. Description
This tool allows to run simple queries in a Spark cluster on Tstat log files.
It instruments jobs coming from a predefined set of possible workflows.
The aim of Tstat-Spark-Miner is to provide a simple command-line tool to extract simple analytics from Tstat log files.
Please read somthing about [Spark](http://spark.apache.org/) and [Tstat](http://tstat.polito.it).

For information about this Readme file and this tool please write to
[martino.trevisan@polito.it](mailto:martino.trevisan@polito.it)

## 2. Prerequisites
To use this tool you need a Spark cluster and a storage source (local or hdfs) where some Tstat log file is store.
You need to be authenticated on the cluster; when using Kerberos authentication, just run:
```
kinit
```
and you will start the authentication process.

Please download locally this tool with this command line:
```
git clone https://github.com/marty90/tstat_spark_miner
```



