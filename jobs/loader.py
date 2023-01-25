from typing import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from .utils import Utility

class Loader(Utility):
    """
    Loader class to load data from CSV files.
    """
    def __init__(self,spark:SparkSession):
        self.version = None # version
        self.spark = spark
        super().__init__()
        
    def __str__(self):
        return ""
    
    def __repr__(self):
        pass
    
    def readCsvFile(self, path:str)->DataFrame:
        return self.spark.read.csv(path, header=True, 
                                   inferSchema=True, sep=",")
    
    def writeCsvFile(self, path:str,Dataframe:DataFrame,mode:str)->bool:
        Dataframe.coalesce(1).write.mode(mode).format('csv').option('header','true').save(path)
        return True