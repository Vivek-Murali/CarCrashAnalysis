import os
import logging 
from .settings import settings  
from pyspark.sql import SparkSession
 

class Logger(object):
    def __init__(self, name,PARENT_DIR):
        """
            Formatting Python log messages
        """
        name = name.replace('.log','')
        logger = logging.getLogger('log_namespace.%s' % name)    # log_namespace can be replaced with your namespace 
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            file_name = os.path.join(os.path.join(PARENT_DIR,settings.LOGGING_DIR), '%s.log' % name)    
            handler = logging.FileHandler(file_name)
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s %(message)s')
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self._logger = logger

    def get(self):
        return self._logger
    
    
class SparkLogger(object):
    """_summary_

    Args:
        object (SparkSession): Use SparkSession to yeild Logs at DEBUG LEVEL.
    """
    def __init__(self, spark:SparkSession):
        """_summary_
            Get spark app details with which to prefix all messages
        Args:
            spark (SparkSession): _description_
        """
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '####<-' + app_name + ' ' + app_id + '->####'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """_summary_
            Spark Logger for Errors
        Args:
            message (String): Message To Log
        """
        self.logger.error(message)

    def warn(self, message):
        """_summary_
            Spark Logger for Warning

        Args:
            message (String): Message To Log
        """
        self.logger.warn(message)
    
    def info(self, message):
        """_summary_
            Spark Logger for information
        Args:
            message (String): Message To Log
        """
        self.logger.info(message)