import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()

import config as cfg
import pyspark.sql.functions as func
# from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
# conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext.getOrCreate();
spark = SparkSession(sc)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def getActivityType(activityCount):
    if activityCount >= cfg.user_activity_types['active']: 
        return 'active'
    elif activityCount >= cfg.user_activity_types['semi-active']: 
        return 'semiactive'
    elif activityCount >= cfg.user_activity_types['rare']: 
        return 'rare'
    elif activityCount >= cfg.user_activity_types['not active']: 
        return 'not active'
    else: 
        return 'null'

activity_type_udf = func.udf(lambda z: getActivityType(int(z)))
genderage_df = spark.read.csv('CLEANED_child_profile.csv', header=True)
def userAggregator(df):
    df = df.join(genderage_df, 'Device ID', 'inner')
    df = df.withColumn('Activity Type', activity_type_udf('Activity Count'))
    df = df.select('Device ID','Time Period', 'Age', 'Gender', 
                   'Activity Type'
                  )
    df = df.groupBy(['Age', 'Gender','Activity Type', 'Time Period']).agg(
     func.count(func.lit(1)).alias("User Counts")
   )
    return df
input_df = spark.read.csv("sample_input.csv",header=True)
result = userAggregator(input_df)
result.write.csv('sample_output')