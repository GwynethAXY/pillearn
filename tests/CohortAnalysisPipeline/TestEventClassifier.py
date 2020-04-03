import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal
from main.CohortAnalysisPipeline.EventClassifier.EventClassifier import EventClassifier

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

class TestEventClassifier(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess(self):
		config = {
		  "main": {
		    "ANALYSIS_DURATION": 90,
		    "BIN_DURATION": 30
		  }
		}
		ec = EventClassifier(config)
		df_pd = pd.DataFrame({'device_id':['00','00','00'],
			'start_time':["2019-01-11T00:00:00","2019-02-11T00:00:00","2019-03-11T00:00:00"],
			'item_name':['A','A','A'],
			'end_time':["2019-01-11T00:00:01","2019-02-11T00:00:01","2019-03-11T00:00:01"],
			'reference':['2019-01-10T00:00:00','2019-01-10T00:00:00','2019-01-10T00:00:00']
			})
		df_spark = self.spark.createDataFrame(df_pd)
		output_spark = ec.process(df_spark)
		output_pd = output_spark.toPandas()
		expected = pd.DataFrame({'device_id':['00','00','00'],
			'start_time':["2019-01-11T00:00:00","2019-02-11T00:00:00","2019-03-11T00:00:00"],
			'item_name':['A','A','A'],
			'end_time':["2019-01-11T00:00:01","2019-02-11T00:00:01","2019-03-11T00:00:01"],
			'bucket':['1','2','3']
			})
		self.assert_frame_equal_with_sort(output_pd,expected,'device_id')

if __name__ == '__main__':
	unittest.main()