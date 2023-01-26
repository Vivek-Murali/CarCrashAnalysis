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
        """__summary__
        Args:
            path (str): Path to the CSV file.
        Returns:
            Dataframe: Dataframe containing the data.
        """
        return self.spark.read.csv(path, header=True, 
                                   inferSchema=True, sep=",")
    
    def writeCsvFile(self, path:str,Dataframe:DataFrame,mode:str)->bool:
        """__summary__
        Args:
            path (str): Path to the CSV file.
            Dataframe (DataFrame): Dataframe containing the data.
            mode (str): Mode to write the data.
        Returns:
            bool: True if the operation was successful
        """
        Dataframe.coalesce(1).write.mode(mode).format('csv').option('header','true').save(path)
        return True