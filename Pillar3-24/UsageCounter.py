#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()

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

#To get spark. working without throwing a NameError
import findspark
findspark.init()
import pyspark # Call this only after findspark
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


#eventClassifierDF can be a dataframe or the directory of a CSV file
def usageCounter(eventClassifierDF, toCSV = False):
    #event Classifier DF has features Device ID, Content ID(ID of song, story, etc.), Time Period
    #Columns must be in order specified above
    
    #if CSV directory is inputted, otherwise assumes a Dataframe was inputted
    if (type(eventClassifierDF) == str):
        schema = StructType([StructField("Device ID", IntegerType(), True),StructField("Content ID", IntegerType(), True),
                            StructField("Time Period", StringType(), True)])
        pd_df = pd.read_csv(eventClassifierDF)
        pd_df.columns = ["Extra", "Device ID", "Time Period", "Activity Count"]
        pd_df = pd_df.drop(["Extra"], axis = 1) #has an extra column for some reason, getting rid of it
        eventClassifierDF = spark.createDataFrame(pd_df, schema=schema)
    
    
    
    #make sure columns have correct names
    oldColumns = eventClassifierDF.schema.names
    df = eventClassifierDF.withColumnRenamed(
        oldColumns[0], "Device ID").withColumnRenamed( #device id
        oldColumns[1], "Content ID").withColumnRenamed( #content id
        oldColumns[2], "Time Period") #time period
    
    if(csv): #if we want to write dataframe to CSV
        df.toPandas().to_csv('Output.csv')
    else: #if we want to return a dataframe
        return df.groupBy("Device ID", "Time Period").agg(count("*")).withColumnRenamed("count(1)", "Activity Count")

