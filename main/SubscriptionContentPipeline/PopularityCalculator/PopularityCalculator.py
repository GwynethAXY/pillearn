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
def main(inputFile, outputFile, configFile, contentMapping):
    df = spark.read.parquet(inputFile+'/*').dropDuplicates().na.drop()
    contentMapping = spark.read.parquet(contentMapping+'/*') #right formatting?
    
    #get rid of ".mp3" in item_name
    df = df.withColumnRenamed("item_name", "to_del")
    df = df.withColumn("item_name", F.split(df['to_del'], '\.')[0])
    df = df.drop('to_del')
    
    #turn Content Mapping String Length to TimeDelta Object
    strp_time = udf (lambda x: datetime.strptime(x, "%M:%S"))
    time_delta = udf (lambda y: timedelta(minutes = y.minute, seconds = y.second))
    
    contentMapping = contentMapping.withColumn("strptime", strp_time(F.col("Length")))
    contentMapping = contentMapping.withColumn("Content Length", time_delta(F.col("strptime")))
    contentMapping = contentMapping.drop('strpTime')
    contentMapping = contentMapping.withColumnRenamed("Title", "item_name")
    
    #Merge df and contentMapping
    df = df.join(contentMapping, ["item_name"], "outer")
    
    #get time played for
    df = df.withColumn("Played For", F.unix_timestamp(df["end"]) - F.unix_timestamp(df["start"]))
    
    #get total seconds of song as String, convert to bigInt
    df = df.withColumn("Song Duration Str", F.regexp_extract(df["Content Length"], "(?<=total: )(.*)(?= seconds)", 0))
    df = df.withColumn("Song Duration Int", df["Song Duration Str"].cast(IntegerType()))
    

    #Let's get Percentage Played
    df = df.withColumn("PercentPlayed", df["Played For"] / df["Song Duration Int"])
    
    #Let's keep only the columns we need at this point
    df = df.select(["device_id", "item_name", "PercentPlayed"])

    #assign weights based on Percent Played
    df = df.withColumn(
    'weight',
    F.when((F.col("PercentPlayed") >= 0.0) & (F.col("PercentPlayed") < 0.25), 0.0)\
    .when((F.col("PercentPlayed") >= 0.25) & (F.col("PercentPlayed") < 0.50), 0.33)\
    .when((F.col("PercentPlayed") >= 0.50) & (F.col("PercentPlayed") < 0.75), 0.66)\
    .otherwise(1.0)
    )

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
    parser.add_argument('-cm', '--contentmapping', required=True) #added on, so contentMapping is required
    args = parser.parse_args()
    config = ConfigParser()
    config.read(args.config)
    main(args.input,args.output,config, args.contentmapping)

