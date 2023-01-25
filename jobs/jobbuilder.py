from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark
from pyspark.sql import DataFrame
from typing import *
from .job import Job
from .loader import Loader
from .logger import Logger
from .settings import settings
import os


class JobBuilder(Job):
    def __init__(self,config:dict,sparkSession:SparkSession):
        self.spark = sparkSession
        self.load = Loader(self.spark)
        self.PARENTDIR = config['variables']['APPDIR']
        self.logger = Logger("Runner",self.PARENTDIR)
        self.config = config
        super().__init__()
        # self.spark.sparkContext.setLogLevel("ERROR")
        # self.spark.sparkContext.setLogLevel("WARN")
        # self.spark.sparkContext.setLogLevel("INFO")
        self.spark.sparkContext.setLogLevel("DEBUG")
        
    def extractionData(self, questionId:str)->bool:
        try:
            if questionId==None:
                return None
            
            if questionId=="1":
                self.logger._logger.info("Extraction->1")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["1"][0]))
                self.logger._logger.info("Processing->1")
                tempDataframe:DataFrame =  self.spark.createDataFrame([self.maleAccidents(personDataframe=tempDataframe)], "integer").toDF("NO_OF_MALE_KILLED_CRASH") 
                self.logger._logger.info("Saving->1")
                self.load.writeCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['destination_path']),"Question_1.csv"),tempDataframe,self.config['functions']['mode'])
                return True
            
            
            """ elif self.questionId=="2":
                self.logger.info("Extraction->2")
                return self.twoWheelerBooked(*args, **kwds)
            elif self.questionId=="3":
                self.logger.info("Extraction->3")
                return self.stateFemaleAccident(*args, **kwds)
            elif self.questionId=="4":
                self.logger.info("Extraction->4")
                return self.topContributionVEH(*args,**kwds)
            elif self.questionId=="5":
                self.logger.info("Extraction->5")
                return self.bodyStyleWiseCrash(*args,**kwds)
            elif self.questionId=="6":
                self.logger.info("Extraction->6")
                return self.topZipcodeAlcohol(*args,**kwds)
            elif self.questionId=="7":
                self.logger.info("Extraction->7")
                return self.noDamageProperty(*args,**kwds)
            elif self.questionId=="8":
                self.logger.info("Extraction->8")
                return self.complexVEHMake(*args,**kwds) """
        except Exception as e:
            self.logger.error(e)
            return None
        finally:
            pass
         
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.extractionData(self.config['functions']['question_id'])