import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal
from main.CohortAnalysisPipeline.UserAggregator.UserAggregator import UserAggregator

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

class TestUserAggregator(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess(self):
		config = {
			"main":{
				"BIN_DURATION":30
			},
		  "activity": {
		    "ACTIVE":1.0,
		    "SEMI-ACTIVE":0.5,
		    "RARE":0.2,
		    "INACTIVE":0.0
		    }
		}
		ua = UserAggregator(config)
		df_pd = pd.DataFrame({'device_id':['00','00','00','01','01','01','02','02','02','03','03','03','04','04','04','05','05','05'],
			'bucket':['1','2','3','1','2','3','1','2','3','1','2','3','1','2','3','1','2','3'],
			'count':[30.0,10.0,1.0,30.0,10.0,1.0,30.0,10.0,1.0,30.0,10.0,1.0,30.0,10.0,1.0,30.0,10.0,1.0]
			})
		profile_pd = pd.DataFrame({'device_id':['00','01','02','03','04','05'],
			'gender':['M','M','M','F','F','F'],
			'age':['1','2','3','4','5','6']})
		df_spark = self.spark.createDataFrame(df_pd)
		profile_spark = self.spark.createDataFrame(profile_pd)
		output_spark = ua.process(df_spark,profile_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'age':['1','1','1','2','2','2','3','3','3','4','4','4','5','5','5','6','6','6'],
			'gender':['M','M','M','M','M','M','M','M','M','F','F','F','F','F','F','F','F','F'],
			'activity_type':['active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive'],
			'bucket':['1','2','3','1','2','3','1','2','3','1','2','3','1','2','3','1','2','3'],
			'count':[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]})
		self.assert_frame_equal_with_sort(output_pd,expected,['age','gender','activity_type','bucket'])


if __name__ == '__main__':
	unittest.main()