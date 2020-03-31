from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, udf
import pyspark.sql.types as T 
import argparse
from configparser import ConfigParser
import json

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

def getActivityType(activityCount, ACTIVE_THRESHOLD, SEMI_ACTIVE_THRESHOLD, RARE_THRESHOLD, INACTIVE_THRESHOLD):
    if activityCount >= ACTIVE_THRESHOLD: 
      return 'active'
    elif activityCount >= SEMI_ACTIVE_THRESHOLD: 
        return 'semiactive'
    elif activityCount >= RARE_THRESHOLD: 
        return 'rare'
    elif activityCount >= INACTIVE_THRESHOLD: 
        return 'inactive'
    else: 
        return 'null'

def main(inputDir,profileMap,outputDir,config):
    ACTIVE_THRESHOLD = config['activity']['ACTIVE']
    SEMI_ACTIVE_THRESHOLD = config['activity']['SEMI-ACTIVE']
    RARE_THRESHOLD = config['activity']['RARE']
    INACTIVE_THRESHOLD = config['activity']['INACTIVE']
    activity_type_udf = udf(lambda count, active,semi,rare,inactive: getActivityType(int(count),active,semi,rare,inactive))
    profile = spark.read.csv(profileMap, header='true')
    df = spark.read.parquet(inputDir+"/*")
    df = df.join(profile, 'device_id')
    df = df.withColumn('activity_type', activity_type_udf(df['count'],lit(ACTIVE_THRESHOLD),lit(SEMI_ACTIVE_THRESHOLD),lit(RARE_THRESHOLD),lit(INACTIVE_THRESHOLD)))
    df = df.select('device_id','bucket', 'age', 'gender', 'activity_type')
    df = df.groupBy(['age', 'gender','activity_type', 'bucket']).agg(F.count(lit(1)).alias("count"))
    df.write.parquet(outputDir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-m', '--map', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
  with open('../config.json') as f:
      config = json.load(f)
    main(args.input,args.map,args.output,config)