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

## 3. Running an simple query
A simple query is the easiest operation you can do on log files.
It takes as input a set of log files and selects a subset of the lines to be written as output using a configurable filter.
The syntax is as follows:
```
```
The query argument can be any combination of column names of the selected log file such as: c_ip, s_ip, fqdn, c_pkts_all, etc...
It must use a python-like syntax and must return a Boolean value

# 3.1 Examples
To select all the lines in the tcp_complete log where the FQDN is `www.facebook.com`, you can type:
```
path='.../2016_11_27_*/log_tcp_complete.gz'
spark-submit  --master yarn-client advanced_query.py -i $path -o "facebook_flows" \
              --query="fqdn=='www.facebook.com'"
```

To select all the urls on server port 7547, you can use:
```
path='.../2016_11_27_*/log_http_complete.gz'
spark-submit  --master yarn-client advanced_query.py -i $path -o "port_7547" \
              --query="s_port=='7547'"
```
Please note that all fields are strings. If you want to evaluate them as integer or float, you must explicitely convert them.

## 4. Running an advanced query
This kind of query is more complex than the previous one since it includes a filter a map and a reduce stage.
Three kinds of 


