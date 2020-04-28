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

conf = SparkConf()
conf.setAppName('pillar')
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf = conf);
spark = SparkSession(sc)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

class PopularityAggregator(object):
	def __init__(self):
		pass

	def process(self,df,profile):
		df = df.join(profile, 'device_id')
		df = df.select('item_name', 'weight', 'age', 'gender')
		df = df.groupBy(['item_name', 'age', 'gender']).agg(F.sum("weight").alias("weighted_sum"))
		return df

def main(inputDir,profileMap,outputDir):
	pa = PopularityAggregator()
	profile = spark.read.csv(profileMap, header='true')
	df = spark.read.parquet(inputDir+"/*")
	output = pa.process(df, profile)
	output.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-m', '--map', required=True)
	parser.add_argument('-o', '--output', required=True)
	parser.add_argument('-c', '--config', required=True)
	args = parser.parse_args()
	main(args.input,args.map,args.output)
