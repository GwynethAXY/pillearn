from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
import argparse
import json

from main.CohortAnalysisPipeline.Utils import splitDate,formDate 	

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

class ShiftFactorExtractor(object):
	def __init__(self, config):
		self.CUTOFF_DATE = config['main']['CUTOFF_DATE']
		self.REFERENCE_DATE = config['main']['REFERENCE_DATE']
		self.ANALYSIS_DURATION = int(config['main']['ANALYSIS_DURATION'])
		# self.df = spark.read.parquet(inputFile+'/*')

	def process1(self,df):
		def getShiftFactor(date,refDate):
			year,month,day = splitDate(date)
			refYear,refMonth,refDay = splitDate(refDate)
			if year == refYear:
				return month - refMonth
			else:
				return int(month + (year-refYear)*12 - refMonth)
		def getSpecificCutoff(date,analysisDuration):
			d = int(analysisDuration)//30
			year,month,day = splitDate(date)
			time = date.split('T')[1]
			newMonth = (month + d) % 12
			if newMonth < month:
				year += 1
			if newMonth == 0:
				newMonth = 12
			return formDate(year,newMonth,day,time)

		get_shift_udf = udf(lambda date, refDate: getShiftFactor(date,refDate))
		get_cutoff_udf = udf(lambda date, analysisDuration: getSpecificCutoff(date,analysisDuration))
		ftu = df.filter(df['event']=='DEVICE_FIRST_TIME_CONNECTED').filter(df['time']<self.CUTOFF_DATE).filter(df['time'] > self.REFERENCE_DATE).select('device_id','time')
		ftu = ftu.withColumn('shift_factor',get_shift_udf(ftu['time'],lit(self.REFERENCE_DATE)))
		ftu = ftu.selectExpr('device_id as device_id2','shift_factor','time as setup_time')
		ftu = ftu.withColumn('cutoff',get_cutoff_udf(ftu['setup_time'],lit(self.ANALYSIS_DURATION)))
		return ftu
	
	def process2(self,df,ftu):
		def shiftDate(date,shiftFactor):
			shiftFactor = int(shiftFactor)
			year,month,day = splitDate(date)
			time = date.split('T')[1]
			if shiftFactor > month:
				yearShift = 1 + (shiftFactor - month) // 12
				year -= yearShift
				month = 12 - (shiftFactor - month) % 12
			else:
				month -= shiftFactor
			return formDate(year,month,day,time)
		def getSpecificRefDate(date,shiftFactor):
			year,month,day = splitDate(date)
			time = date.split('T')[1]
			month = (month-int(shiftFactor)) % 12
			if month == 0:
				month = 12
			return formDate(year,month,day,time)
		shift_udf = udf(lambda date, shiftFactor: shiftDate(date, shiftFactor))
		get_ref_udf = udf(lambda date,shift: getSpecificRefDate(date,shift))
		df = df.join(ftu, [df.device_id == ftu.device_id2]).drop('device_id2')
		df = df.filter(df['time']<=df['cutoff']).filter(df['time']>=df['setup_time'])
		df = df.withColumn('time_shifted', shift_udf(df['time'],df['shift_factor']))
		df = df.withColumn('reference', get_ref_udf(df['setup_time'],df['shift_factor']))
		df = df.drop('cutoff','shift_factor','setup_time')
		return df
	def process(self,df):
		ftu = self.process1(df)
		output = self.process2(df,ftu)
		return output

def main(inputDir, outputDir, config):
	sfe = ShiftFactorExtractor(config)
	df = spark.read.parquet(inputDir+'/*')
	output = sfe.process(df)
	output.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser() 
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-o', '--output', required=True)
	parser.add_argument('-c', '--config', required=True)
	args = parser.parse_args()
	with open('config.json') as f:
	    config = json.load(f)
	main(args.input,args.output,config)



