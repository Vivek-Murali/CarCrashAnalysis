from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark


class JobBuilder(object):
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()