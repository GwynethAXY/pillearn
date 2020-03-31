from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
import argparse
from configparser import ConfigParser

from Utils.Utils import splitDate, formDate

conf = SparkConf()
conf.setAppName('pillar')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sql_context = SQLContext(sc)
spark = SparkSession.builder.appName('pillar').getOrCreate()

def getShiftFactor(date,refDate):
	year,month,day = splitDate(date)
	refYear,refMonth,refDay = splitDate(refDate)
	if year == refYear:
		return month - refMonth
	else:
		return int(month + (year-refYear)*12 - refMonth)

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

def getSpecificCutoff(date,analysisDuration):
	d = int(analysisDuration)//30
	year,month,day = splitDate(date)
	time = date.split('T')[1]
	month = (month + d) % 12
	if month == 0:
		month = 12
	return formDate(year,month,day,time)

def getSpecificRefDate(date,shiftFactor):
	year,month,day = splitDate(date)
	time = date.split('T')[1]
	month = (month-int(shiftFactor)) % 12
	if month == 0:
		month = 12
	return formDate(year,month,day,time)

def main(inputDir, outputDir, configFile):
	CUTOFF_DATE = configFile.get('main','CUTOFF_DATE')
	REFERENCE_DATE = configFile.get('main','REFERENCE_DATE')
	ANALYSIS_DURATION = int(configFile.get('main','ANALYSIS_DURATION'))

	get_shift_udf = udf(lambda date, refDate: getShiftFactor(date,refDate))
	shift_udf = udf(lambda date, shiftFactor: shiftDate(date, shiftFactor))
	get_cutoff_udf = udf(lambda date, analysisDuration: getSpecificCutoff(date,analysisDuration))
	get_ref_udf = udf(lambda date,shift: getSpecificRefDate(date,shift))
	
	df = spark.read.parquet(inputDir+'/*')
	ftu = df.filter(df['event']=='DEVICE_FIRST_TIME_CONNECTED').filter(df['time']<CUTOFF_DATE).filter(df['time'] > REFERENCE_DATE).select('device_id','time')
	ftu = ftu.withColumn('shift_factor',get_shift_udf(ftu['time'],lit(REFERENCE_DATE)))
	ftu = ftu.selectExpr('device_id as device_id2','shift_factor','time as setup_time')
	ftu = ftu.withColumn('cutoff',get_cutoff_udf(ftu['setup_time'],lit(ANALYSIS_DURATION)))
	df = df.join(ftu, [df.device_id == ftu.device_id2]).drop('device_id2')
	df = df.filter(df['time']<=df['cutoff']).filter(df['time']>=df['setup_time'])
	df = df.withColumn('time_shifted', shift_udf(df['time'],df['shift_factor']))
	df = df.withColumn('reference', get_ref_udf(df['setup_time'],df['shift_factor']))
	df = df.drop('cutoff','shift_factor','setup_time')
	df.write.parquet(outputDir)

if __name__ == "__main__":
	parser = argparse.ArgumentParser() 
	parser.add_argument('-i', '--input', required=True)
	parser.add_argument('-o', '--output', required=True)
	parser.add_argument('-c', '--config', required=True)
	args = parser.parse_args()
	config = ConfigParser()
	config.read(args.config)
	main(args.input,args.output,config)