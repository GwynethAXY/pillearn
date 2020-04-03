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

class TestCLASSNAME(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess(self):
		# init class
		classToTest = ClassName(arg1, arg2)
		# test input data
		df_pd = pd.DataFrame({})
		# convert pandas DF to spark DF
		df_spark = self.spark.createDataFrame(df_pd)
		# call class function to be tested
		output_spark = classToTest.process(df_spark)
		# convert output spark DF to pandas DF
		output_pd = output_spark.toPandas()
		# expected output
		expected = pd.DataFrame({'age':['1','1','1','2','2','2','3','3','3','4','4','4','5','5','5','6','6','6'],
			'gender':['M','M','M','M','M','M','M','M','M','F','F','F','F','F','F','F','F','F'],
			'activity_type':['active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive','active','rare','inactive'],
			'bucket':['1','2','3','1','2','3','1','2','3','1','2','3','1','2','3','1','2','3'],
			'count':[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]})
		# assert output == expected output
		self.assert_frame_equal_with_sort(output_pd,expected,['COLUMN1','COLUMN2'])


if __name__ == '__main__':
	unittest.main()