#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### common pyspark import statements
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf 
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.functions import split, explode
### other essnetial import statements
import argparse
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta

#my imports
from pyspark.sql.types import IntegerType

'''
	Standardize code to the following structure: everything to be in functions except pyspark envrionment setup
		- environment setup
		- user defined functions
		- main function
		- program start point
''' 

# setup
conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

# main function: should always take in input file, output file and config file
def main(inputFile, outputFile, configFile):

    df = spark.read.parquet(inputFile+'/*').dropDuplicates().na.drop()
    
     #DF for Item Play Started
    start = df.filter(col('event').isin(['ITEM_PLAY_STARTED']))
    #split json string and get just the name
    start = start.withColumn('Content Name', split(df['payload'], ':')[2])
    name_UDF = udf(lambda x:x[1:-2],StringType()) 
    start = start.withColumn('Content Name', name_UDF('Content Name'))
    #turn Time Start String to easy to use Time Stamp Object
    start = start.withColumn('Time Start', df['time'].cast("timestamp"))
    #Select just the columns we need
    cols = ["device_id", "Content Name", "Time Start"]
    start = start.select(*cols)
    
    
    #DF for Item Play Finished
    finished = df.filter(col('event').isin(['ITEM_PLAY_FINISHED']))
    #split json string and get just the name
    finished = finished.withColumn('Content Name', split(df['payload'], ':')[3])
    finished = finished.withColumn('Content Name', name_UDF('Content Name'))
    
    #split json string and get whether content was played in entirety
    finished = finished.withColumn('reach_end_of_stream', split(df['payload'], ':')[1])
    stream_end_UDF = udf(lambda x:x[0:5],StringType()) 
    finished = finished.withColumn('reach_end_of_stream', stream_end_UDF('reach_end_of_stream'))
    
    #Get rid of white space
    finished = finished.withColumn("reach_end_of_stream", trim(col("reach_end_of_stream")))
    
    #Convert True/False strings to actual boolean values
    finished = finished.withColumn(
    'reach_end_of_stream',
    F.when(F.col("reach_end_of_stream") == "true", True)\
    .otherwise(False)
    )
    #turn Time End String to easy to use Time Stamp Object
    finished = finished.withColumn('Time End', df['time'].cast("timestamp"))
    #Select just the columns we need
    cols = ["device_id", "Content Name", "Time End", "reach_end_of_stream"]
    finished = finished.select(*cols)
    
    #combine two dataframes for our transformed Schema
    transformed = start.join(finished, on=["device_id", "Content Name"], how='left_outer')
    
    #Make sure Time Start before time end
    transformed = transformed.where(col("Time Start") <= col("Time End"))
    
    #Convert time stamps to unix
    #transformed = transformed.withColumn('Time Start', F.unix_timestamp('Time Start'))
    #transformed = transformed.withColumn('Time End', F.unix_timestamp('Time End'))
    
    #Get correct Time Ends
    def getEndTime(end_time_list):
        return end_time_list[0]
    
    end_time_udf = udf(lambda end_time_list: end_time_list[0], TimestampType())
    df = transformed.withColumn("end_time_list", F.collect_list("Time End").over(Window.partitionBy("device_id",'Content Name','Time Start', "reach_end_of_stream").orderBy('Time End')))
    df = df.groupBy('device_id','Time Start','Content Name', "reach_end_of_stream").agg(F.max('end_time_list').alias('end_time_list'))
    #Still gets laggy here running the udf that takes first item of list (aka the smallest date time)
    df = df.withColumn('Time End', end_time_udf("end_time_list"))
    df = df.drop('end_time_list')
    
    #rename columns + reorder
    df = df.withColumnRenamed("Time Start", "start").withColumnRenamed("Time End", "end").withColumnRenamed("Content Name", "item_name")
    df = df.select("device_id", "item_name", "start", "end", "reach_end_of_stream")

    df.write.parquet(outputFile) # Write onto output Parquet


# program start point
if __name__ == "__main__":
    ''' 
        Add arguments to script during execution on command line
        Example of how to run the script: spark-submit template.py -i input.parquet -o output.parquet -c config.ini
    '''
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    config = ConfigParser()
    config.read(args.config)
    main(args.input,args.output,config)

