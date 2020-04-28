from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf 
from pyspark.sql.functions import lit
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

#config accessor
class popularityCalculator(object):
    def __init__(self, config):
        self.first = float(config['weights']['FIRST'])
        self.second = float(config['weights']['SECOND'])
        self.third = float(config['weights']['THIRD'])
        self.fourth = float(config['weights']['FOURTH'])

def main(inputFile, outputFile, configFile, contentMapping):
    
    #config
    uc = popularityCalculator(configFile)
    
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
    F.when((F.col("PercentPlayed") >= 0.0) & (F.col("PercentPlayed") < 0.25), uc.first)\
    .when((F.col("PercentPlayed") >= 0.25) & (F.col("PercentPlayed") < 0.50), uc.second)\
    .when((F.col("PercentPlayed") >= 0.50) & (F.col("PercentPlayed") < 0.75), uc.third)\
    .when((F.col("PercentPlayed") >= 0.75) & (F.col("PercentPlayed") <= 1.00), uc.fourth)\
    .otherwise(-999.999)
    )
    #drop the rows with invalid percent played
    df = df.filter((df.weight != -999.999))

    df.write.parquet(outputFile) # Write onto output Parquet


if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-c', '--config', required=True)
    parser.add_argument('-cm', '--contentmapping', required=True) #added on, so contentMapping is required
    args = parser.parse_args()
    with open(args.config) as f:
        config = json.load(f)
    main(args.input,args.output,config, args.contentmapping)

