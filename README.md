# Tstat-Spark-Miner
This tool allows to perform simple queries to a *Spark* based cluster where *Tstat* log files are stored.

## Index
[comment]: <> (Done with https://github.com/ekalinin/github-markdown-toc )
   * [1. Description](#1-description)
   * [2. Prerequisites](#2-prerequisites)
      * [2.1 Getting Started](#21-getting-started)
      * [2.2 Driver allocation](#22-driver-allocation)
      * [2.3 Input path specification](#23-input-path-specification)
      * [2.4 Expression syntax](#24-expression-syntax)
   * [3. Running a simple query (select specific lines)](#3-running-a-simple-query-select-specific-lines)
      * [3.1 Examples](#31-examples)
         * [3.1.1 Log lines to Facebook](#311-log-lines-to-facebook)
         * [3.1.2 HTTP requests to port 7547](#312-http-requests-to-port-7547)
   * [4. Running an advanced query (map   reduce)](#4-running-an-advanced-query-map--reduce)
      * [4.1 Examples](#41-examples)
         * [4.1.1 Clients downloading large HTTP files](#411-clients-downloading-large-http-files)
         * [4.1.2 Server IPs contacted with QUIC protocol](#412-server-ips-contacted-with-quic-protocol)
         * [4.1.3 Domain Popularity](#413-domain-popularity)
         * [4.1.4 Domain Rank](#414-domain-rank)
         * [4.1.5 Average Flow size](#415-average-flow-size)
         
# 1. Description
This tool allows to run simple queries in a *Spark* cluster if *Tstat* log files are stored.
It instruments jobs coming from a predefined set of possible workflows.
The aim of *Tstat-Spark-Miner* is to provide a command-line tool to extract simple analytics from *Tstat* log files.
Please read something about [Spark](http://spark.apache.org/) and [Tstat](http://tstat.polito.it).

For information about this Readme file and this tool please write to
[martino.trevisan@polito.it](mailto:martino.trevisan@polito.it)

# 2. Prerequisites
## 2.1 Getting Started
To use this tool you need a *Spark* cluster and a storage source (local or hdfs) where some *Tstat* log file is store.
You need to be authenticated on the cluster; when using *Kerberos* authentication, just run:
```
kinit
```
and you will start the authentication process.

Please download locally this tool with this command line:
```
git clone https://github.com/marty90/tstat_spark_miner
```
## 2.2 Driver allocation

You must run this *Spark* application with the spark-submit tool.
Please rember to set the `--master` option in the correct way;
it can be `yarn-client` (driver process is executed on the local machine)
or `yarn-cluster` (driver process is executed on a cluster node).

By default *Tstat-Spark-Miner* stores the result of a query on the `hdfs` file system.
With `-l` option, the result is saved on the local file system too;
for obvious reasons, this is allowed only in `yarn-client` mode.

## 2.3 Input path specification
The input path for log files will be given as input to the Spark environment;
therefore it can contain wildcards (\*) and expansions (in *bash-like* syntax, e.g., `log_\{1,2\}` will be expanded in `log_1` and `log_2` ).
The path can refer to *HDFS* files and in this case it will be something like the following (it includes all *log_tcp_complete* in November 2016):
```
hdfs://BigDataHA/data/DET/POLI/2016/11-Nov/2016_11_*/log_tcp_complete.gz
```
It can refer to the local file system as well (when running the driver locally), using this syntax:
```
file:///<absolute_path_to_files>
```

## 2.4 Expression syntax
This tool heavily uses `eval` and `exec` *python builtins*; so, be careful when writing your command line.
All arguments must be python expressions; if you are not familiar with *python* (2.7), please read [something](https://docs.python.org/2/).

# 3. Running a simple query (select specific lines)
A simple query is the easiest operation you can do on log files.
It takes as input a set of log files and **selects a subset of the lines** to be written as output using a configurable filter.
The selected lines will be written in the output with all their fields.
The syntax is as follows:
```
spark-submit simple_query.py [-h] [-i input] [-o output] [-q query] [-s separator]
                       [-l]

optional arguments:
  -h, --help            show this help message and exit
  -i input, --input input
                        Input log files path.
  -o output, --output output
                        Output file where the result of the query is written.
  -q query, --query query
                        Python expression to select a specific line.
  -s separator, --separator separator
                        Separator field in log files, in letters. Can be
                        space|tab. Default is space.
  -l, --copy_to_local   Copy the result file in the local file system.

```
The query argument can be any combination of column names of the selected log file such as: `c_ip`, `s_ip`, `fqdn`, `c_pkts_all`, etc...
The fields of the log files are accesible also using the fields variable which is a simple list of fields:
e.g., `fields[0]` is the `c_ip` in the *log_tcp_complete* while `fields[6]` is the `hostname` in the *log_http_complete*.
It must use a *python*-like syntax and must return a *Boolean* value.

## 3.1 Examples
### 3.1.1 Log lines to Facebook
To select all the lines in the tcp_complete log where the *FQDN* is `www.facebook.com`, you can type:
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit  simple_query.py -i $path -o "facebook_flows" \
              --query "fqdn=='www.facebook.com'"
```
### 3.1.2 HTTP requests to port 7547
To select all the urls on server port 7547, you can use:
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit  simple_query.py -i $path -o "port_7547" -s tab \
              --query "s_port=='7547'"
```
Please note that all fields are strings. If you want to evaluate them as integer or float, you must explicitely convert them.

# 4. Running an advanced query (map + reduce)
This kind of query is more complex than the previous one since it **includes a *filter*, a *map* and a *reduce* transformation.**
Optionally, you can specify a second map transformation if you use *ReduceByKey* that will be executed after the latter.
Three kinds of workflows are allowed.
* *Filter* -> *Map* -> *Distinct*
* *Filter* -> *Map* -> *Reduce*
* *Filter* -> *Map* -> *ReduceByKey* ( -> *Map*)

The command line syntax is:
```
spark-submit advanced_query.py [-h] [-i input] [-o output] [--filter filter]
                         [--map map] [--distinct] [--reduce reduce]
                         [--reduceByKey reduceByKey] [--finalMap finalMap]
                         [--separator separator] [-l]
optional arguments:
  -h, --help            show this help message and exit
  -i input, --input input
                        Input log files path.
  -o output, --output output
                        Output file where the result of the query is written.
  --filter filter       Filter input log lines (Python expression).
  --map map             Map each line after filter (Python expression).
  --distinct            Get distinct elements after map (Python expression).
  --reduce reduce       Reduce after map. Implicit arguments are v1 and v2.
                        (Python expression).
  --reduceByKey reduceByKey
                        ReduceByKey after map. Implicit arguments are v1 and
                        v2. (Python expression).
  --finalMap finalMap   Map stage after ReduceByKey. Implicit arguments are k
                        for the key and v for the value. (Python expression).
  --separator separator
                        Separator field in log files, in letters. Can be
                        space|tab. Default is space.
  -l, --copy_to_local   Copy the result file in the local file system.
```

## 4.1 Examples
### 4.1.1 Clients downloading large HTTP files
With this command line you get the list of client IP addresses downloading files larger than 1MB
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit  advanced_query.py -i $path -o "domain_rank" -s tab \
             --filter "method == 'HTTP' and int(fields[7])>5000" --map="c_ip" \
             --distinct
```
Please note that so far, the tool uses the first line of each log as header line.
Thus, to get the fields in the HTTP response lines, you must use the indexed access with fields variable.
### 4.1.2 Server IPs contacted with QUIC protocol
This query creates the list of Server IP address that are contacted using the QUIC protocol over UDP.
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit advanced_query.py -i $path -o "quic_s_ip" \
             --filter "c_type=='27' and s_type=='27'" \
             --map "s_ip" \
             --distinct
```
### 4.1.3 Domain Popularity
This example counts how many flow are directed to each domain (*FQDN*).
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit advanced_query.py -i $path -o "domain_pop" \
             --filter "fqdn!='-'" \
             --map "(fqdn,1)" \
             --reduceByKey "v1+v2"
```

### 4.1.4 Domain Rank
This examples calculates the rank of the domain names in the log. The rank is the number of users (source IP addresses)
accessing a domain.
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit advanced_query.py -i $path -o "domain_rank" \
             --filter "fqdn!='-'" \
             --map "(fqdn,{c_ip})" \
             --reduceByKey "v1|v2" \
             --finalMap "k + ' ' + str(len(v))"
```
### 4.1.5 Average Flow size
This command line calculates the average flow size of facebook.com subdomains.
```
path='hdfs://.../2016_11_27_*/log_tcp_complete.gz'
spark-submit  advanced_query.py -i $path -o "average_size" \
              --filter "'facebook.com' in fqdn" \
              --map "(fqdn,(int(s_bytes_uniq),1))" \
              --reduceByKey "(v1[0] + v2[0], v1[1] + v2[1])"
              --finalMap "k + ' ' + str(v[0]/v[1])"
```
