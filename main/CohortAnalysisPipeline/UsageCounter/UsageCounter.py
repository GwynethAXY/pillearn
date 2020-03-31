import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime
import pyspark.sql.types as T 
from pyspark.sql.functions import split, explode

import argparse
from configparser import ConfigParser

from Utils.Utils import getDuration

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

def convertLength(length):
    return int(length.split(':')[0])*60+int(length.split(':')[1])

def countForActive(duration,length, durationThreshold, percentageThreshold):
    if int(duration) < int(length) and int(length) > durationThreshold and int(duration)/int(length) > percentageThreshold:
        return 1
    elif int(duration) >= int(length):
        return 1
    else:
        return 0

def percentageActive(numActive,numTotal):
    return int(numActive)/int(numTotal)

def durationCheck(duration,length):
    return not (int(duration)>int(length) or int(duration)<0)

def main(inputDir, contentMap, outputDir, config):
    LONG_ITEM_DURATION_THRESHOLD = int(config['usage']['LONG_ITEM_DURATION_THRESHOLD'])
    LONG_ITEM_ACTIVE_PERCENTAGE = float(config['usage']['LONG_ITEM_ACTIVE_PERCENTAGE'])
    duration_udf = udf(lambda start, end: getDuration(start,end))
    length_udf = udf(lambda length: convertLength(length))
    count_active_udf = udf(lambda duration,length, durationThreshold, percentageThreshold: countForActive(duration,length, durationThreshold, percentageThreshold))
    percentage_udf = udf(lambda numActive, numTotal: percentageActive(numActive,numTotal))
    duration_check_udf = udf(lambda duration, length: durationCheck(duration,length))
    content_map = spark.read.csv(contentMap,header='true').select('title','length')
    df = spark.read.parquet(inputDir+'/*').dropDuplicates().na.drop()
    df = df.join(content_map,[df.item_name == content_map.title])
    df = df.withColumn('duration', duration_udf(df['start_time'],df['end_time']))
    df = df.withColumn('length', length_udf(df['length']))
    df = df.withColumn('check',duration_check_udf(df['duration'],df['length']))
    df = df.filter(df['check']=='true').drop('check')
    df = df.withColumn('counts_for_active', count_active_udf(df['duration'],df['length'],lit(LONG_ITEM_DURATION_THRESHOLD),lit(LONG_ITEM_ACTIVE_PERCENTAGE)))
    df = df.groupBy('device_id','bucket').agg(F.sum('counts_for_active').alias('count'))
    df.write.parquet(outputDir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-m', '--map', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    with open('config.json') as f:
        config = json.load(f)
    main(args.input,args.map,args.output,config)
