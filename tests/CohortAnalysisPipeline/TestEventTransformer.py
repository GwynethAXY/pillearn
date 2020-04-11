import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal
from main.CohortAnalysisPipeline.EventTransformer.EventTransformer import EventTransformer

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

class TestEventTransformer(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess1(self):
		et = EventTransformer()
		df_pd = pd.DataFrame({'device_id':['00','00','00','00'],
			'event':['DEVICE_FIRST_TIME_CONNECTED','ITEM_PLAY_STARTED','ITEM_PLAY_FINISHED','ITEM_PLAY_FINISHED'],
			'payload':['','{"item_name":"A.mp3"}','{"item_name":"A.mp3"}','{"item_name":"A.mp3"}'],
			'time':["2019-08-10T00:00:00","2019-10-11T00:00:00","2019-10-11T00:00:01","2019-10-11T00:00:02"],
			'time_shifted':["2019-01-10T00:00:00","2019-03-11T00:00:00","2019-03-11T00:00:01","2019-03-11T00:00:02"],
			'reference':["2019-01-10T00:00:00","2019-01-10T00:00:00","2019-01-10T00:00:00","2019-01-10T00:00:00"]})
		df_spark = self.spark.createDataFrame(df_pd)
		sec_output_spark, main_output_spark = et.process1(df_spark)
		output_pd = main_output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['00','00'],
			'start_time':["2019-03-11T00:00:00","2019-03-11T00:00:00"],
			'item_name':['A','A'],
			'device_id2':['00','00'],
			'end_time':["2019-03-11T00:00:01","2019-03-11T00:00:02"],
			'item_name2':['A','A']})
		self.assert_frame_equal_with_sort(output_pd,expected,'device_id')

	def testProcess2(self):
		et = EventTransformer()
		ref_pd = pd.DataFrame({'device_id2':['00'],'reference':['2019-01-10T00:00:00']})
		df_pd = pd.DataFrame({'device_id':['00','00'],
			'start_time':["2019-03-11T00:00:00","2019-03-11T00:00:00"],
			'item_name':['A','A'],
			'device_id2':['00','00'],
			'end_time':["2019-03-11T00:00:01","2019-03-11T00:00:02"],
			'item_name2':['A','A']})		
		df_spark = self.spark.createDataFrame(df_pd)
		ref_spark = self.spark.createDataFrame(ref_pd)
		output_spark = et.process2(df_spark,ref_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['00'],
			'start_time':["2019-03-11T00:00:00"],
			'item_name':['A'],
			'end_time':["2019-03-11T00:00:01"],
			'reference':['2019-01-10T00:00:00']
			})
		self.assert_frame_equal_with_sort(output_pd,expected,'device_id')

if __name__ == '__main__':
	unittest.main()