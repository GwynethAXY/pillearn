import unittest
import logging
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal

## import class
from main.SubscriptionContentPipeline.PopularityAggregator.PopularityAggregator import PopularityAggregator

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

class TestPopularityAggregator(PySparkTest):
	def assert_frame_equal_with_sort(self,results, expected, keycolumns):
		results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
		expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
		assert_frame_equal(results_sorted, expected_sorted)

	def testProcess(self):
		# init class
		pa = PopularityAggregator()
		# test input data
		df_pd = pd.DataFrame({'device_id':['00','00','00','01','01','01','02','02','02','03','03','03','04','04','04','05','05','05'],
			'item_name':['Pop Goes the Weasel2','Pop Goes the Weasel2','Humpty Dumpty','Pop Goes the Weasel2','Humpty Dumpty','Pop Goes the Weasel2','Humpty Dumpty','Humpty Dumpty','Pop Goes the Weasel2','Pop Goes the Weasel2','Pop Goes the Weasel2','Pop Goes the Weasel2','Humpty Dumpty','Pop Goes the Weasel2','The Wheels on the Bus','Five Little Monkeys','Humpty Dumpty','Humpty Dumpty'],
			'PercentPlayed':[0.3,0.1,1.0,0.6,0.5,0.2,1.0,0.1,1.0,0.3,0.75,0.7,0.9,0.1,1.0,0.3,0.1,1.0],
            'weight':[0.33,0.0,1.0,0.66,0.33,0.0,1.0,0.0,1.0,0.33,0.66,0.66,1.0,0.0,1.0,0.33,0.0,1.0]
			})
		profile_pd = pd.DataFrame({'device_id':['00','01','02','03','04','05'],
			'gender':['M','M','M','F','F','M'],
			'age':['1','2','3','4','1','1']})
		# convert pandas DF to spark DF
		df_spark = self.spark.createDataFrame(df_pd)
		profile_spark = self.spark.createDataFrame(profile_pd)
		# call class function to be tested
		output_spark = pa.process(df_spark, profile_spark)
		# convert output spark DF to pandas DF
		output_pd = output_spark.toPandas()
		# expected output
		expected = pd.DataFrame({'item_name':['Five Little Monkeys','Humpty Dumpty','Humpty Dumpty','Humpty Dumpty','Humpty Dumpty','Pop Goes the Weasel2', 'Pop Goes the Weasel2', 'Pop Goes the Weasel2', 'Pop Goes the Weasel2', 'Pop Goes the Weasel2', 'The Wheels on the Bus'],
			'age':['1', '1', '1', '2', '3', '1', '1', '2', '3', '4', '1'],
			'gender':['M','F','M','M','M','F','M','M','M','F','F'],
			'weighted_sum':[0.33, 1.0, 2.0, 0.33, 1.0, 0.0, 0.33, 0.66, 1.0, 1.65, 1.0]})
		# assert output == expected output
		self.assert_frame_equal_with_sort(output_pd,expected,['item_name', 'age', 'gender', 'weighted_sum'])


if __name__ == '__main__':
	unittest.main()
