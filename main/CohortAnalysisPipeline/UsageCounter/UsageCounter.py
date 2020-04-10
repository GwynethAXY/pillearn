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
from main.CohortAnalysisPipeline.Utils import getDuration

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

class UsageCounter(object):
    def __init__(self, config):
        self.LONG_ITEM_DURATION_THRESHOLD = int(config['usage']['LONG_ITEM_DURATION_THRESHOLD'])
        self.LONG_ITEM_ACTIVE_PERCENTAGE = float(config['usage']['LONG_ITEM_ACTIVE_PERCENTAGE'])
    
    def process1(self,df,content_map):
        def convertLength(length):
            return int(length.split(':')[0])*60+int(length.split(':')[1])

        def durationCheck(duration,length):
            return not (int(duration)>int(length) or int(duration)<0)

        duration_udf = udf(lambda start, end: getDuration(start,end))
        length_udf = udf(lambda length: convertLength(length))
        duration_check_udf = udf(lambda duration, length: durationCheck(duration,length))
        content_map = content_map.select('title','length')
        df = df.join(content_map,[df.item_name == content_map.title])
        df = df.withColumn('duration', duration_udf(df['start_time'],df['end_time']))
        df = df.withColumn('length', length_udf(df['length']))
        df = df.withColumn('check',duration_check_udf(df['duration'],df['length']))
        df = df.filter(df['check']=='true').drop('check')
        return df
    
    def process2(self,df):
        def countForActive(duration,length, durationThreshold, percentageThreshold):
            if int(duration) < int(length) and int(length) >= durationThreshold and int(duration)/int(length) >= percentageThreshold:
                return 1
            elif int(duration) >= int(length):
                return 1
            else:
                return 0
        count_active_udf = udf(lambda duration,length, durationThreshold, percentageThreshold: countForActive(duration,length, durationThreshold, percentageThreshold))
        df = df.withColumn('counts_for_active', count_active_udf(df['duration'],df['length'],lit(self.LONG_ITEM_DURATION_THRESHOLD),lit(self.LONG_ITEM_ACTIVE_PERCENTAGE)))
        df = df.groupBy('device_id','bucket').agg(F.sum('counts_for_active').alias('count'))
        return df 

    def process(self,df,content_map):
        df = self.process1(df,content_map)
        df = self.process2(df)
        return df

def main(inputDir, contentMap, outputDir, config):
    uc = UsageCounter(config)
    content_map = spark.read.csv(contentMap,header='true')
    df = spark.read.parquet(inputDir+'/*').dropDuplicates().na.drop()
    output = uc.process(df,content_map)
    output.write.parquet(outputDir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-m', '--map', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    with open(args.config) as f:
        config = json.load(f)
    main(args.input,args.map,args.output,config)
