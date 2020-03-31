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

from Utils.Utils import splitDate, formDate

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

def getBucket(date, refDate, analysisDuration, binDuration):
	year,month,day = splitDate(date)
	refYear,refMonth,refDay = splitDate(refDate)
	bucket = int((year*365+month*30+day-refYear*365-refMonth*30-refDay) // binDuration + 1)
	return bucket

def main(inputDir, outputDir, config):
	ANALYSIS_DURATION = int(config['main']['ANALYSIS_DURATION'])
	BIN_DURATION = int(config['main']['BIN_DURATION'])
	df = spark.read.parquet(inputDir+'/*')
	bucket_udf = udf(lambda date, refDate, analysisDuration, binDuration: getBucket(date, refDate, analysisDuration, binDuration))
	df = df.withColumn('bucket',bucket_udf(df['start_time'],df['reference'],lit(ANALYSIS_DURATION), lit(BIN_DURATION)))
	df = df.drop('reference')
	df.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser() 
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-o', '--output', required=True)
	parser.add_argument('-c', '--config', required=True)
	args = parser.parse_args()
	with open('config.json') as f:
	    config = json.load(f)
	main(args.input,args.output,config)
