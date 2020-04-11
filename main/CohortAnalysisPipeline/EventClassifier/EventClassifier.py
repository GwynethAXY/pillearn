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
import sys; sys.path = [''] + sys.path
from main.CohortAnalysisPipeline.Utils import splitDate,formDate 	

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

class EventClassifier(object):
	def __init__(self,config):
		self.ANALYSIS_DURATION = int(config['main']['ANALYSIS_DURATION'])
		self.BIN_DURATION = int(config['main']['BIN_DURATION'])

	def process(self,df):
		def getBucket(date, refDate, analysisDuration, binDuration):
			year,month,day = splitDate(date)
			refYear,refMonth,refDay = splitDate(refDate)
			bucket = int((year*365+month*30+day-refYear*365-refMonth*30-refDay) // binDuration + 1)
			return bucket
		bucket_udf = udf(lambda date, refDate, analysisDuration, binDuration: getBucket(date, refDate, analysisDuration, binDuration))
		df = df.withColumn('bucket',bucket_udf(df['start_time'],df['reference'],lit(self.ANALYSIS_DURATION), lit(self.BIN_DURATION)))
		df = df.drop('reference')
		return df

def main(inputDir, outputDir, config):
	ec = EventClassifier(config)
	df = spark.read.parquet(inputDir+'/*')
	output = ec.process(df)
	output.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser() 
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-o', '--output', required=True)
	parser.add_argument('-c', '--config', required=True)
	args = parser.parse_args()
	with open(args.config) as f:
	    config = json.load(f)
	main(args.input,args.output,config)
