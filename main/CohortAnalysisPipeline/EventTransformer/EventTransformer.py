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

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

class EventTransformer(object):
	def process1(self,df):
		def getItemName(payload):
		    payload = json.loads(payload)
		    return payload['item_name'].split('.')[0]

		item_udf = udf(lambda payload: getItemName(payload))
		ref = df.selectExpr('device_id as device_id2','reference').distinct()
		item_start = df.filter(df['event']=='ITEM_PLAY_STARTED')
		item_start = item_start.withColumn('item_name',item_udf(item_start['payload']))
		item_start = item_start.selectExpr('device_id','time_shifted as start_time','item_name')
		item_end = df.filter(df['event']=='ITEM_PLAY_FINISHED')
		item_end = item_end.withColumn('item_name',item_udf(item_end['payload']))
		item_end = item_end.selectExpr('device_id as device_id2','time_shifted as end_time','item_name as item_name2')
		cond = [item_start.item_name == item_end.item_name2, item_start.device_id == item_end.device_id2, item_start.start_time < item_end.end_time]
		df = item_start.join(item_end,cond)
		return ref, df

	def process2(self,df,ref):
		def getEndTime(end_time_list):
			return end_time_list[0]

		end_time_udf = udf(lambda end_time_list: getEndTime(end_time_list))
		df = df.withColumn("end_time_list", F.collect_list("end_time").over(Window.partitionBy("device_id",'item_name','start_time').orderBy('end_time')))
		df = df.groupBy('device_id','start_time','item_name').agg(F.max('end_time_list').alias('end_time_list'))
		df = df.withColumn('end_time',end_time_udf(df['end_time_list']))
		df = df.drop('end_time_list')
		df = df.join(ref,[df.device_id == ref.device_id2]).drop('device_id2')
		return df

	def process(self,df):
		ref, df = self.process1(df)
		output = self.process2(df,ref)
		return output

def main(inputDir, outputDir):
	et = EventTransformer()
	df = spark.read.parquet(inputDir+'/*')
	output = et.process(df)
	output.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser() 
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-o', '--output', required=True)
	args = parser.parse_args()
	main(args.input,args.output)