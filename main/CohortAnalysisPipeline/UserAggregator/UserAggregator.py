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

class UserAggregator(object):
    def __init__(self,config):
        self.ACTIVE_THRESHOLD = float(config['activity']['ACTIVE'])
        self.SEMI_ACTIVE_THRESHOLD = float(config['activity']['SEMI-ACTIVE'])
        self.RARE_THRESHOLD = float(config['activity']['RARE'])
        self.INACTIVE_THRESHOLD = float(config['activity']['INACTIVE'])
        self.BIN_DURATION = float(config['main']['BIN_DURATION'])

    def process(self,df,profileMap):
        def getActivityType(activityCount, ACTIVE_THRESHOLD, SEMI_ACTIVE_THRESHOLD, RARE_THRESHOLD, INACTIVE_THRESHOLD,BIN_DURATION):
            if activityCount >= ACTIVE_THRESHOLD*BIN_DURATION: 
              return 'active'
            elif activityCount >= SEMI_ACTIVE_THRESHOLD*BIN_DURATION: 
                return 'semiactive'
            elif activityCount >= RARE_THRESHOLD*BIN_DURATION: 
                return 'rare'
            elif activityCount >= INACTIVE_THRESHOLD*BIN_DURATION: 
                return 'inactive'
            else: 
                return 'null'

        activity_type_udf = udf(lambda count, active,semi,rare,inactive,binDuration: getActivityType(int(count),active,semi,rare,inactive,binDuration))
        df = df.join(profileMap, 'device_id')
        df = df.withColumn('activity_type', activity_type_udf(df['count'],lit(self.ACTIVE_THRESHOLD),lit(self.SEMI_ACTIVE_THRESHOLD),lit(self.RARE_THRESHOLD),lit(self.INACTIVE_THRESHOLD),lit(self.BIN_DURATION)))
        df = df.select('device_id','bucket', 'age', 'gender', 'activity_type')
        df = df.groupBy(['age', 'gender','activity_type', 'bucket']).agg(F.count(lit(1)).alias("count"))
        return df

def main(inputDir,profileMap,outputDir,config):
    ua = UserAggregator(config)
    profile = spark.read.csv(profileMap, header='true')
    df = spark.read.parquet(inputDir+"/*")
    output = ua.process(df,profile)
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