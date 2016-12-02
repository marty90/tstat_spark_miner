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

query=""
separator = ""

def main():
    global query
    global separator
    
    # Create config to connect with the cluster
    conf = (SparkConf()
             .setAppName("Tstat Spark Miner - Simple Query")
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
    parser.add_argument('-q', '--query', metavar='query', type=str,
                       help='Python expression to select a specific line.')  
    parser.add_argument('-s', '--separator', metavar='separator', type=str, default="space", 
                       help='Separator field in log files, in letters. Can be space|tab. Default is space.')
    parser.add_argument('-l','--copy_to_local', metavar='copy_to_local', default=False, action='store_const', const=True,
                       help='Copy the result file in the local file system.')
                                                      
    args = vars(parser.parse_args())
    path=args["input"]
    output_file=args["output"]
    copy_to_local=args["copy_to_local"]
    query=args["query"]
    if args["separator"] == "space":
        separator=" "
    elif args["separator"] == "tab":
        separator="\t"
    else:
        print ("Invalid separator. Must be space|tab")
        return

    log = sc.textFile(path).mapPartitions(mapLog)
    
    
    call ("hdfs dfs -rm -r " + output_file, shell=True)
    log.saveAsTextFile(output_file)
    if copy_to_local:
        call ("hdfs dfs -getmerge " + output_file + " " + output_file , shell=True)
    
def mapLog(lines):
    global query
    line_number=0
    query_compiled=compile( "result=" + query, '<string>', 'exec')
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
                exec query_compiled
                if result:
                    yield line
            except:
                pass

if __name__ == "__main__":
    main()


