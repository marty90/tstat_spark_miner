#!/usr/bin/python3
# TO BE EXECUTED WITH:
# spark-submit hello.py
#    or
# time spark-submit hello.py

import operator
import sys
import argparse
from subprocess import call
from pyspark import SparkConf, SparkContext


separator = ""
filter_func=""
map_func=""
reduce_func=""
reduceByKey_func=""
distinct_flag=""
    
def main():

    global separator
    global filter_func
    global map_func
    global reduce_func
    global reduceByKey_func
    global distinct_flag
    
    # Create config to connect with the cluster
    conf = (SparkConf()
             .setAppName("Tstat Spark Miner - Advanced Query")
             .set("spark.dynamicAllocation.enabled", "false")
             .set("spark.task.maxFailures", 128)
             .set("spark.yarn.max.executor.failures", 128)
             .set("spark.executor.memory", "7G")
             .set("spark.executor.instances", "564")
             .set("spark.network.timeout", "300")
    )
    sc = SparkContext(conf = conf)
    
    parser = argparse.ArgumentParser(description='Simple Tstat log files query tool')                                                      
    parser.add_argument('-i', '--input', metavar='input', type=str,
                       help='Input log files path.')                       
    parser.add_argument('-o', '--output', metavar='output', type=str,
                       help='Output file where the result of the query is written.') 
                       
    parser.add_argument('--filter', metavar='filter', type=str,
                       help='Filter input log lines (Python expression).')  
    parser.add_argument('--map', metavar='map', type=str,
                       help='Map each line after filter (Python expression).')      
                                        

    parser.add_argument('--distinct', metavar='distinct', default=False, action='store_const', const=True,
                       help='Get distinct elements after map (Python expression).') 
    parser.add_argument('--reduce', metavar='reduce', type=str, default="",
                       help='Reduce after map. Implicit arguments are v1 and v2. (Python expression).') 
    parser.add_argument('--reduceByKey', metavar='reduceByKey', type=str, default="",
                       help='ReduceByKey after map. Implicit arguments are v1 and v2. (Python expression).')

    parser.add_argument('--finalMap', metavar='finalMap', type=str, default="",
                       help='Map stage after ReduceByKey. Implicit arguments are k for the key and v for the value. (Python expression).')                       
                                                                   
                       
    parser.add_argument('--separator', metavar='separator', type=str, default="space", 
                       help='Separator field in log files, in letters. Can be space|tab. Default is space.')
                       
    parser.add_argument('-l','--copy_to_local', metavar='copy_to_local', default=False, action='store_const', const=True,
                       help='Copy the result file in the local file system.')
                                                      
    args = vars(parser.parse_args())
    path=args["input"]
    output_file=args["output"]
    copy_to_local=args["copy_to_local"]
    
    filter_func=args["filter"]
    map_func=args["map"]
    
    reduce_func=args["reduce"]
    reduceByKey_func=args["reduceByKey"]
    distinct_flag=args["distinct"]
    finalMap_func=args["finalMap"]
    
    not_null=0
    if reduce_func != "":
        not_null+=1
        mode="reduce"
    if reduceByKey_func != "":
        not_null+=1
        mode="reduceByKey"
    if distinct_flag:
        not_null+=1
        mode="distinct"
    if not_null != 1:
        print ("Exactly one among reduce, reduceByKey and distinct must be set.")
        return        
    
    if args["separator"] == "space":
        separator=" "
    elif args["separator"] == "tab":
        separator="\t"
    else:
        print ("Invalid separator. Must be space|tab")
        return

    log_mapped = sc.textFile(path).mapPartitions(mapLog)
    
    if mode == "distinct":
        log = log_mapped.distinct()   
            
    if mode == "reduce":
        _reduce_lambda = eval('lambda v1,v2: ' + reduce_func)
        log = sc.parallelize( log_mapped.reduce(_reduce_lambda) )  
        
    if mode == "reduceByKey":
        _reduce_lambda = eval('lambda v1,v2: ' + reduceByKey_func)
        if finalMap_func!="":
            _map_lambda = eval('lambda (k,v): ' + finalMap_func)
        else:
            _map_lambda = lambda (k,v): str (k) + " " + str (v)
        log = log_mapped.reduceByKey(_reduce_lambda).map ( _map_lambda )
        
    call ("hdfs dfs -rm -r " + output_file, shell=True)
    log.saveAsTextFile(output_file)
    if copy_to_local:
        call ("hdfs dfs -getmerge " + output_file + " " + output_file , shell=True)            
    
def mapLog(lines):
    global filter_func
    global map_func
    global separator
    
    line_number=0
    filter_compiled=compile( "_result=" + filter_func, '<string>', 'exec')
    map_compiled=compile( "_mapped=" + map_func, '<string>', 'exec')    
    for line in lines:
        line_number += 1
        if line_number==1:
            line_new = line.split("#")[-1]
            fields = [ x.split(":")[0] for x in line_new.split(separator)]
            byte_codes = [ compile( f + "=elem", '<string>', 'exec') for f in fields ]
        else:
            fields = line.split(separator)
            for i, elem in enumerate (fields):
                if i<len(byte_codes):
                    exec byte_codes[i]
            try:
                exec filter_compiled
                if _result:
                    exec map_compiled
                    yield _mapped
            except:
                pass

if __name__ == "__main__":
    main()


