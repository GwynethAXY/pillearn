import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal
from main.CohortAnalysisPipeline.UsageCounter.UsageCounter import UsageCounter

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

class TestUsageCounter(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess1(self):
		config = {
		  "usage": {
		    "LONG_ITEM_DURATION_THRESHOLD": 120,
		    "LONG_ITEM_ACTIVE_PERCENTAGE": 0.5
		  }
		}
		uc = UsageCounter(config)
		df_pd = pd.DataFrame({'device_id':['00','00','00'],
			'start_time':["2019-01-11T00:00:00","2019-02-11T00:00:00","2019-03-11T00:00:00"],
			'item_name':['A','B','C'],
			'end_time':["2019-01-11T00:05:00","2019-02-11T00:10:00","2019-03-11T00:00:30"],
			'bucket':['1','2','3']
			})
		map_pd = pd.DataFrame({'title':['A','B','C'],'length':['10:00','30:00','0:20']})

		df_spark = self.spark.createDataFrame(df_pd)
		map_spark = self.spark.createDataFrame(map_pd)
		output_spark = uc.process1(df_spark,map_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['00','00'],
			'start_time':["2019-01-11T00:00:00","2019-02-11T00:00:00"],
			'item_name':['A','B'],
			'end_time':["2019-01-11T00:05:00","2019-02-11T00:10:00"],
			'bucket':['1','2'],
			'title':['A','B'],
			'length':['600','1800'],
			'duration': ['300','600']
			})
		self.assert_frame_equal_with_sort(output_pd,expected,['device_id','start_time','item_name'])

	def testProcess2(self):
		config = {
		  "usage": {
		    "LONG_ITEM_DURATION_THRESHOLD": 120,
		    "LONG_ITEM_ACTIVE_PERCENTAGE": 0.5
		  }
		}
		uc = UsageCounter(config)
		df_pd = pd.DataFrame({'device_id':['00','00'],
			'start_time':["2019-01-11T00:00:00","2019-02-11T00:00:00"],
			'item_name':['A','B'],
			'end_time':["2019-01-11T00:05:00","2019-02-11T00:10:00"],
			'bucket':['1','2'],
			'title':['A','B'],
			'length':['600','1800'],
			'duration': ['300','600']
			})
		df_spark = self.spark.createDataFrame(df_pd)
		output_spark = uc.process2(df_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['00','00'],
			'bucket':['1','2'],
			'count':[1.0,0.0]
			})
		self.assert_frame_equal_with_sort(output_pd,expected,['device_id','bucket'])


if __name__ == '__main__':
	unittest.main()