from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark
from typing import *


class JobBuilder(object):
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sparkContext.setLogLevel("INFO")
        self.spark.sparkContext.setLogLevel("DEBUG")
        
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass