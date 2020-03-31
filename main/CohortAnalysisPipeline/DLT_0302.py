import sys
import pyspark as ps
import warnings
import re
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

# configurations

CUTOFF_DATE = "2019-12"
REFERENCE_DATE = "2019-01"
ANALYSIS_DURATION = 90
BIN_DURATION = 30

# get events from new users before REFERENCE_DATE

def getShiftFactor(date,refDate):
	month = int(date.split('T')[0].split('-')[1])
	year = int(date.split('T')[0].split('-')[0])
	refMonth = int(refDate.split('-')[1])
	refYear = int(refDate.split('-')[0])
	if year == refYear:
		return month - refMonth
	else:
		return int(month + (year-refYear)*12 - refMonth)

get_shift_udf = udf(lambda date, refDate: getShiftFactor(date,refDate))

df = spark.read.parquet('./device_logs/*')
first_time_users = df.filter(df['event']=='DEVICE_FIRST_TIME_CONNECTED').filter(df['time']<CUTOFF_DATE).filter(df['time'] > REFERENCE_DATE).select('device_id','time')
first_time_users = first_time_users.withColumn('shift_factor',get_shift_udf(df['time'],lit(REFERENCE_DATE)))
first_time_users.write.parquet('DLT_01_users_shift_table')
first_time_users = first_time_users.selectExpr('device_id as device_id2')
df = df.join(first_time_users,[df.device_id == first_time_users.device_id2]).drop('device_id2')
df.write.parquet('DLT_01_users_device_logs')

# for these users, transform the dataset into this schema: device_id, item_name, start_time, end_time

def getItemName(payload):
    payload = json.loads(payload)
    return payload['item_name'].split('.')[0]

def getEndTime(end_time_list):
	return end_time_list[0]

item_udf = udf(lambda payload: getItemName(payload))
end_time_udf = udf(lambda end_time_list: getEndTime(end_time_list))

df = spark.read.parquet('./DLT_01_users_device_logs/*')
item_start = df.filter(df['event']=='ITEM_PLAY_STARTED')
item_start = item_start.withColumn('item_name',item_udf(item_start['payload']))
item_start = item_start.selectExpr('device_id','time as start_time','item_name')
item_end = df.filter(df['event']=='ITEM_PLAY_FINISHED')
item_end = item_end.withColumn('item_name',item_udf(item_end['payload']))
item_end = item_end.selectExpr('device_id as device_id2','time as end_time','item_name as item_name2')
cond = [item_start.item_name == item_end.item_name2, item_start.device_id == item_end.device_id2, item_start.start_time < item_end.end_time]
df = item_start.join(item_end,cond)
df = df.withColumn("end_time_list", F.collect_list("end_time").over(Window.partitionBy("device_id",'item_name','start_time').orderBy('end_time')))
df = df.groupBy('device_id','start_time','item_name').agg(F.max('end_time_list').alias('end_time_list'))
df = df.withColumn('end_time',end_time_udf(df['end_time_list']))
df = df.drop('end_time_list')
df.write.parquet('DLT_02_users_device_logs_transformed')

# put records into buckets (month 1, 2, 3) according to REFERENCE_DATE

def shiftDate(date,shiftFactor):
	shiftFactor = int(shiftFactor)
	day = date.split('T')[0].split('-')[2]
	month = int(date.split('T')[0].split('-')[1])
	year = int(date.split('T')[0].split('-')[0])
	time = date.split('T')[1]
	if shiftFactor > month:
		yearShift = 1 + (shiftFactor - month) // 12
		year -= yearShift
		month = 12 - (shiftFactor - month) % 12
	else:
		month -= shiftFactor
	if month < 10:
		month = '0'+str(month)
	date = '-'.join([str(year),str(month),day])
	return date+"T"+time

def getMonthBucket(date, REFERENCE_DATE, ANALYSIS_DURATION):
	date = date.split('T')[0].split('-')
	refDate = REFERENCE_DATE.split('-')
	diff = int(date[0])*12+int(date[1]) - int(refDate[0])*12 - int(refDate[1])
	if diff > ANALYSIS_DURATION:
		return None
	else:
		return diff

shift_udf = udf(lambda date, shiftFactor: shiftDate(date, shiftFactor))
month_udf = udf(lambda date, REFERENCE_DATE, ANALYSIS_DURATION: getMonthBucket(date,REFERENCE_DATE,ANALYSIS_DURATION))
df = spark.read.parquet('./DLT_02_users_device_logs_transformed/*').dropDuplicates()
shift_table = spark.read.parquet('./DLT_01_users_shift_table/*').selectExpr('device_id as device_id2','shift_factor')
df = df.join(shift_table,[df.device_id == shift_table.device_id2]).drop('device_id2','time')
df = df.withColumn('start_time_shifted', shift_udf(df['start_time'],df['shift_factor']))
df = df.withColumn('end_time_shifted', shift_udf(df['end_time'],df['shift_factor']))
df = df.withColumn('month', month_udf(df['start_time_shifted'],lit(REFERENCE_DATE),lit(ANALYSIS_DURATION)))
df = df.filter(df['month']>0).na.drop()
df = df.drop('start_time_shifted','end_time_shifted','shift_factor')
df.write.parquet('DLT_03_users_device_logs_shifted')

# apply function to calculate usage, determine active users by month

def getDuration(start,end):
	start = dateToSeconds(start)
	end = dateToSeconds(end)
	return int(end - start)

def dateToSeconds(date):
	date = " ".join(date.split('.')[0].split('T'))
	return (datetime.strptime(date, '%Y-%m-%d %H:%M:%S')-datetime(1970,1,1)).total_seconds()

def convertLength(length):
	return int(length.split(':')[0])*60+int(length.split(':')[1])

def countForActive(duration,length):
	if int(duration) < int(length) and int(length) > 120 and int(duration)/int(length) > 0.5:
		return 1
	elif int(duration) >= int(length):
		return 1
	else:
		return 0

def percentageActive(numActive,numTotal):
	return int(numActive)/int(numTotal)

duration_udf = udf(lambda start, end: getDuration(start,end))
length_udf = udf(lambda length: convertLength(length))
count_active_udf = udf(lambda duration,length: countForActive(duration,length))
percentage_udf = udf(lambda numActive, numTotal: percentageActive(numActive,numTotal))

content_map = spark.read.csv('../content_mapping.csv',header='true').select('title','length')
df = spark.read.parquet('./DLT_03_users_device_logs_shifted/*').dropDuplicates().na.drop()
df = df.join(content_map,[df.item_name == content_map.title])
df = df.withColumn('duration', duration_udf(df['start_time'],df['end_time']))
df = df.withColumn('length', length_udf(df['length']))
df = df.withColumn('counts_for_active', count_active_udf(df['duration'],df['length']))
df = df.groupBy('device_id','month').agg(F.sum('counts_for_active').alias('count'))
active = df.filter(df['count']>=20)
non_active = df.filter(df['count']<20)
active = active.groupBy('month').agg(F.count('device_id').alias('active')).orderBy('month')
non_active = non_active.groupBy('month').agg(F.count('device_id').alias('user_counts')).orderBy('month').selectExpr('month as month2','user_counts as non_active')
all_users = active.join(non_active,[active.month == non_active.month2]).drop('month2').orderBy('month')

all_users.coalesce(1).write.parquet('DTL_04_users_by_month')








