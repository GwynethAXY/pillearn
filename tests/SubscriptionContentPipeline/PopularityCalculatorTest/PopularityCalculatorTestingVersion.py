#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Takes in/outputs dataframes instead of Parquets to make testing more convenient


# In[ ]:


#my imports, to make sure it works fine on Jupyter Notebook

import sys
import warnings
import re
import json
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql.context import SQLContext
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import lit
# from pyspark.sql.window import Window
# import pyspark.sql.functions as F
# from datetime import datetime
# import pyspark.sql.types as T 
# from pyspark.sql.functions import split, explode

#To get spark. working without throwing a NameError
import findspark
findspark.init()
import pyspark as ps # Call this only after findspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# In[ ]:


#ACTUAL FUNCTION

#for renaming the columns
from functools import reduce

#allow us to use SQL Count function for aggregation in groupBY
from pyspark.sql.functions import count

#allow us to read csv to dataframe
import pandas as pd #need Pandas
from pyspark.sql.types import *


# In[ ]:


### common pyspark import statements
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf 
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.functions import split, explode
### other essnetial import statements
import argparse
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta

#my imports
from pyspark.sql.types import IntegerType
def main(inputFile, outputFile, configFile, contentMapping):
    #df = spark.read.parquet(inputFile+'/*').dropDuplicates().na.drop()
    df = inputFile #is just a df
    #contentMapping = spark.read.parquet(contentMapping+'/*') #right formatting?
    
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
    F.when((F.col("PercentPlayed") >= 0.0) & (F.col("PercentPlayed") < 0.25), 0.0)\
    .when((F.col("PercentPlayed") >= 0.25) & (F.col("PercentPlayed") < 0.50), 0.33)\
    .when((F.col("PercentPlayed") >= 0.50) & (F.col("PercentPlayed") < 0.75), 0.66)\
    .when((F.col("PercentPlayed") >= 0.75) & (F.col("PercentPlayed") <= 1.00), 0.66)\
    .otherwise(-999.999)
    )
    #drop the rows with invalid percent played
    df = df.filter((df.weight != -999.999))

    #df.write.parquet(outputFile) # Write onto output Parquet
    return df #return changed df


# In[ ]:




