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
    """
        JobBuilder with options
    """
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
        
    def extractionData(self, questionId:int):
        """_summary_
            Case Statement for performing the ETL pipeline.
        Args:
            questionId (str): Question identifier for the extraction.
        Returns:
            None 
        """ 
        try:
            if questionId==None:
                return None
            
            elif questionId==1:
                self.logger._logger.info("Extraction->1")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["1"][0]))
                self.logger._logger.info("Processing->1")
                tempDataframe:DataFrame =  self.spark.createDataFrame([self.maleAccidents(personDataframe=tempDataframe)], "integer").toDF("NO_OF_MALE_KILLED_CRASH") 
                questionFileName:str = "Question_1"
            
            elif questionId==2:
                self.logger._logger.info("Extraction->2")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["2"][0]))
                self.logger._logger.info("Processing->2")
                tempDataframe:DataFrame =  self.spark.createDataFrame([self.twoWheelerBooked(unitsDataframe=tempDataframe)], "integer").toDF("TWO_WHEELER_CRASH_CNT")
                questionFileName:str = "Question_2"
                
            elif questionId==3:
                self.logger._logger.info("Extraction->3")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["3"][0]))
                self.logger._logger.info("Processing->3")
                tempDataframe:DataFrame =  self.stateFemaleAccident(personDataframe=tempDataframe)
                questionFileName:str = "Question_3"
                
            elif questionId==4:
                self.logger._logger.info("Extraction->4")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["4"][0]))
                self.logger._logger.info("Processing->4")
                tempDataframe:DataFrame =  self.topContributionVEH(unitsDataframe=tempDataframe)
                questionFileName:str = "Question_4"
                
            elif questionId==5:
                self.logger._logger.info("Extraction->5")
                rightDataframe = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["5"][0]))
                leftDataframe = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["5"][1]))
                self.logger._logger.info("Processing->5")
                tempDataframe:DataFrame =  self.bodyStyleWiseCrash(unitsDataframe=leftDataframe,personDataframe=rightDataframe)
                questionFileName:str = "Question_5"
                
            elif questionId==6:
                self.logger._logger.info("Extraction->6")
                tempDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["6"][0]))
                self.logger._logger.info("Processing->6")
                tempDataframe:DataFrame =  self.topZipcodeAlcohol(personDataframe=tempDataframe)
                questionFileName:str = "Question_6"
                
            elif questionId==7:
                self.logger._logger.info("Extraction->7")
                rightDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["7"][0]))
                leftDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["7"][1]))
                centerDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["7"][2]))
                self.logger._logger.info("Processing->7")
                tempDataframe:DataFrame =  self.spark.createDataFrame([self.noDamageProperty(chargesDataframe=rightDataframe,unitsDataframe=centerDataframe,damagesDataframe=leftDataframe)], "integer").toDF("NO_OF_SEVERE_CRASHES")
                questionFileName:str = "Question_7"
                
            elif questionId==8:
                self.logger._logger.info("Extraction->8")
                rightDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["8"][0]))
                leftDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["8"][1]))
                centerDataframe:DataFrame = self.load.readCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['source_path']),settings.QUESTION_MAPPING["8"][2]))
                self.logger._logger.info("Processing->8")
                tempDataframe:DataFrame = self.complexVEHMake(personDataframe=rightDataframe,unitsDataframe=centerDataframe,chargesDataframe=leftDataframe)
                questionFileName:str = "Question_8"
                
        except Exception as e:
            self.logger._logger.error(e)
            return None
        finally:
            self.logger._logger.info("Saving To->{}".format(questionFileName))
            self.load.writeCsvFile(os.path.join(os.path.join(self.PARENTDIR,self.config['resource']['destination_path']),questionFileName),tempDataframe,self.config['functions']['mode'])
            self.logger._logger.info("Done->{}".format(questionFileName))
         
    def __call__(self, *args: Any, **kwds: Any):
        return self.extractionData(self.config['functions']['question_id'])