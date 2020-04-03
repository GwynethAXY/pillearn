import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal
from main.CohortAnalysisPipeline.ShiftFactorExtractor.ShiftFactorExtractor import ShiftFactorExtractor

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

class TestShiftFactorExtractor(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess1(self):
		config = {
		  "main": {
		    "CUTOFF_DATE": "2019-12-01T00:00:00",
		    "REFERENCE_DATE":"2019-01-01T00:00:00",
		    "ANALYSIS_DURATION": 90,
		  }
		}
		sfe = ShiftFactorExtractor(config)
		df_pd = pd.DataFrame({'device_id':['00','01','02','03','04'],
			'event':['DEVICE_FIRST_TIME_CONNECTED','DEVICE_FIRST_TIME_CONNECTED','EVENT1','DEVICE_FIRST_TIME_CONNECTED','DEVICE_FIRST_TIME_CONNECTED'],
			'payload':['','','','',''],
			'time':["2018-08-01T00:00:00","2020-08-01T00:00:00","2019-08-01T00:00:00","2019-08-01T00:00:00","2019-11-01T00:00:00"]})
		df_spark = self.spark.createDataFrame(df_pd)
		output_spark = sfe.process1(df_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id2':['03','04'],'shift_factor':['7','10'],'setup_time':['2019-08-01T00:00:00','2019-11-01T00:00:00'],'cutoff':['2019-11-01T00:00:00','2020-02-01T00:00:00']})
		self.assert_frame_equal_with_sort(output_pd,expected,'device_id2')

	def testProcess2(self):
		config = {
		  "main": {
		    "CUTOFF_DATE": "2019-12-01T00:00:00",
		    "REFERENCE_DATE":"2019-01-01T00:00:00",
		    "ANALYSIS_DURATION": 90,
		  }
		}
		sfe = ShiftFactorExtractor(config)
		df_pd = pd.DataFrame({'device_id':['00','03','03','04','04'],
			'event':['DEVICE_FIRST_TIME_CONNECTED','ITEM_PLAY_STARTED','ITEM_PLAY_STARTED','ITEM_PLAY_STARTED','ITEM_PLAY_STARTED'],
			'payload':['','','','',''],
			'time':["2019-08-01T00:00:00","2018-08-01T00:00:00","2019-10-01T00:00:00","2020-08-01T00:00:00","2019-11-01T00:00:00"]})
		ftu_pd = pd.DataFrame({'device_id2':['03','04'],'shift_factor':['7','10'],'setup_time':['2019-08-01T00:00:00','2019-11-01T00:00:00'],'cutoff':['2019-11-01T00:00:00','2020-02-01T00:00:00']})
		df_spark = self.spark.createDataFrame(df_pd)
		ftu_spark = self.spark.createDataFrame(ftu_pd)
		output_spark = sfe.process2(df_spark,ftu_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['03','04'],
			'event':['ITEM_PLAY_STARTED','ITEM_PLAY_STARTED'],
			'payload':['',''],
			'time':["2019-10-01T00:00:00","2019-11-01T00:00:00"],
			'time_shifted':["2019-03-01T00:00:00","2019-01-01T00:00:00"],
			'reference':['2019-01-01T00:00:00','2019-01-01T00:00:00']
			})
		self.assert_frame_equal_with_sort(output_pd,expected,'device_id')

if __name__ == '__main__':
	unittest.main()