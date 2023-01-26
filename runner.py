from jobs import (JobBuilder, Logger, Utility)
from pyspark.sql import SparkSession



if __name__ == '__main__':
    CONFIG = Utility()
    CONFIG._parseArguments()
    args = CONFIG.getConfig()
    LOG = Logger("Runner",args['variables']['APPDIR'])
    LOG._logger.info(args)
    spark = SparkSession.builder.appName(args.get("app_name")).getOrCreate()
    Runner = JobBuilder(sparkSession=spark,config=args)
    Runner()
    