from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.functions import split, explode
import argparse
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
import json
from pyspark.sql.types import IntegerType

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

def main(inputFile, outputFile):
    def getItemName(payload):
        payload = json.loads(payload)
        return payload['item_name'].split('.')[0]

    item_udf = udf(lambda payload: getItemName(payload))
    df = spark.read.parquet(inputFile+'/*')

     #DF for Item Play Started
    start = df.filter(col('event').isin(['ITEM_PLAY_STARTED']))
    start = start.withColumn('Content Name', item_udf(start['payload'])) # get content name
    start = start.withColumn('Time Start', df['time'].cast("timestamp")) #turn Time Start String to easy to use Time Stamp Object
    cols = ["device_id", "Content Name", "Time Start"]    #Select just the columns we need
    start = start.select(*cols)
    
    def getEndOfStream(payload):
        payload = json.loads(payload)
        if "did_reach_end_of_stream" in payload:
            return payload['did_reach_end_of_stream']
        else:
            return "false"

    stream_end_UDF = udf(lambda x: getEndOfStream(x))
        
        #DF for Item Play Finished
    finished = df.filter(col('event').isin(['ITEM_PLAY_FINISHED'])) 
    finished = finished.withColumn('Content Name', item_udf(finished['payload'])) # get content name
    finished = finished.withColumn('reach_end_of_stream', stream_end_UDF(finished['payload'])) # get did_reach_end_of_stream        
    finished = finished.withColumn("reach_end_of_stream", F.trim(col("reach_end_of_stream"))) #Get rid of white space

        
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
    
    end_time_udf = udf(lambda end_time_list: end_time_list[0], T.TimestampType())
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
    args = parser.parse_args()
    main(args.input,args.output)

