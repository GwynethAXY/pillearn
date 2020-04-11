#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal

## import class
from main.PIPELINE_NAME.MODULE_NAME.FILE_NAME import CLASS_NAME 

class PySparkTest(unittest.TestCase):
	@classmethod
	def suppress_py4j_logging(cls):
		logger = logging.getLogger('py4j')
		logger.setLevel(logging.WARN)
	@classmethod
	def create_testing_pyspark_session(cls):
		return (SparkSession.builder.master('local[*]').appName("pillar").enableHiveSupport().getOrCreate())
	@classmethod
	def setUpClass(cls):
		cls.suppress_py4j_logging()
		cls.spark = cls.create_testing_pyspark_session()
	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()

class TestPopularityCalculator(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess(self):
        # init class
        #pc = PopularityCalculator(arg1, arg2) Don't really need
        # test input data
        from datetime import datetime, timedelta
    df_pd = spark.createDataFrame(
        [#shouldn't matter that I use int for user id's as they are never touched in this function 
        (0, 'Black Beauty Part 1 The Spring Hunt.mp3', 
         datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'), 
         datetime.strptime('Jun 1 2005  1:30PM', '%b %d %Y %I:%M%p'),
        False), # create your data here, be consistent in the types.
        (1, "The Peacock.mp3", 
         datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'), 
         datetime.strptime('Jun 1 2005  1:50PM', '%b %d %Y %I:%M%p'),
        True),
        (1, "Phonics.mp3", 
         datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'), 
         datetime.strptime('Jun 1 2005  1:34PM', '%b %d %Y %I:%M%p'),
        False), # create your data here, be consistent in the types.
        (1, "Pizza Moon.mp3", 
         datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'), 
         datetime.strptime('Jun 1 2005  1:34PM', '%b %d %Y %I:%M%p'),
        True),
        (1, "Pop Goes the Weasel2.mp3", 
         datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'), 
         datetime.strptime('Jun 1 2005  1:34PM', '%b %d %Y %I:%M%p'),
        False), # create your data here, be consistent in the types.
        ],
        ['device_id', 'item_name', "start", "end", "reach_end_of_stream"] # add your columns label here
        )
    content_mapping = spark.createDataFrame([
        ("Black Beauty Part 1 The Spring Hunt", "4:15"),
        ("The Peacock", "2:35"),
        ("Phonics", "4:01"),
        ("Pizza Moon", "3:20"),
        ("Pop Goes the Weasel2", "0:41"),],
        ["Title", "Length"]
        )
#         # call class function to be tested
    output_spark = main(df_pd, "Don't Need for Testing", "Don't Need for Testing", content_mapping)
        # convert output spark DF to pandas DF
        output_pd = output_spark.toPandas() #taking in a parquet, so switch that parquet to pandas dataframe
        # expected output
        expected = pd.DataFrame({'device_id':['1','1'],
            'item_name':['Phonics','Pizza Moon'],
            'PercentPlayed':[0.24896265560165975, 0.3],
            'weight':[0.0,0.33]})
        # assert output == expected output
        self.assert_frame_equal_with_sort(output_pd,expected,['device_id','item_name', 'PercentPlayed', 'weight'])

if __name__ == '__main__':
	unittest.main()

