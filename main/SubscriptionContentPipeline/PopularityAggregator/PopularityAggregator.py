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
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf = conf);
spark = SparkSession(sc)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()
# main function: should always take in input file, output file and config file
def main(inputDir,profileMap,outputDir,config):
    profile = spark.read.csv(profileMap, header='true')
    df = spark.read.parquet(inputDir+"/*")
    df = df.join(profile, 'device_id')
    df = df.select('item_name', 'weight', 'age', 'gender')
    df = df.groupBy(['item_name', 'age', 'gender']).sum().alias('weighted_sum')
    df.write.parquet(outputDir)

# program start point
if __name__ == "__main__":
	'''
		Add arguments to script during execution on command line
		Example of how to run the script: spark-submit template.py -i input.parquet -o output.parquet -c config.ini
	'''
	parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-m', '--map', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    config = ConfigParser()
    config.read(args.config)
    main(args.input,args.map,args.output,config)
