from pyspark.sql.functions import lit, desc, col, size, array_contains\
, isnan, udf, hour, array_min, array_max, countDistinct
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import settings


class Jobs:
    def __init__(self,spark:SparkSession):
        self.spark = spark
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sparkContext.setLogLevel("INFO")
        self.spark.sparkContext.setLogLevel("DEBUG")
        
    def maleAccidents(self,Dataframe:DataFrame)->DataFrame:
        """_summary_
            The No of Occurences of crashes where male persons are killed 
        Args:
            Dataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        return Dataframe.filter("PRSN_GNDR_ID =='MALE' and  DEATH_CNT==1").select('CRASH_ID').distinct().count()
    
    def twoWheelerBooked(self,Dataframe:DataFrame)->DataFrame:
        """_summary_
            The no of occurences where two wheelers were booked for crashes
        Args:
            Dataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        return Dataframe.filter(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%')).count()
    
    def stateFemaleAccident(self,Dataframe:DataFrame)->DataFrame:
        """_summary_
            The state with highest number of accidents where females are involved 
        Args:
            Dataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        tempDataframe = Dataframe.filter("PRSN_GNDR_ID=='FEMALE'").filter(~col('DRVR_LIC_STATE_ID').isin(settings.EXCLUDED_STATES)).select('DRVR_LIC_STATE_ID').groupBy('DRVR_LIC_STATE_ID').count()
        return tempDataframe.withColumn('rn',rank().over(Window.orderBy(col('count').desc()))).filter(col('rn')==1).select('DRVR_LIC_STATE_ID').withColumnRenamed('DRVR_LIC_STATE_ID','MOST_FEM_ACCIDENT_STATE')
    
    def topContributionVEH(self,Dataframe:DataFrame)->DataFrame:
        """_summary_
            The top 5th to 15th most contributing vehicles for crashes to a largest number of injuries including death.
        Args:
            Dataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        combinedDataframe = Dataframe.filter(col('VEH_MAKE_ID')!='NA').withColumn('INJR_DEATH_CNT',col('DEATH_CNT')+col('TOT_INJRY_CNT')) \
                    .withColumn('INJR_DEATH_CNT',col('INJR_DEATH_CNT').cast(IntegerType())).select('VEH_MAKE_ID','INJR_DEATH_CNT')
        aggregatedDataframe = combinedDataframe.groupBy('VEH_MAKE_ID').agg(sum(col('INJR_DEATH_CNT')).alias('TOT_INJ_DEATH_CNT'))
        return aggregatedDataframe.withColumn('rn',row_number().over(Window.orderBy(col('TOT_INJ_DEATH_CNT').desc()))).filter("rn >=5 and rn <=15") \
                                  .select('VEH_MAKE_ID').withColumnRenamed('VEH_MAKE_ID','MOST_INJR_DEATH_VEH_MAKE_ID')
    
    
    def topZipcodeAlcohol(self,Dataframe:DataFrame)->DataFrame:
        """_summary_
            The top 5 Zipcodes alcohol contributing vehicles crash.
        Args:
            Dataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        tempDataframe = Dataframe.filter("PRSN_ALC_RSLT_ID == 'Positive' and DRVR_ZIP is not null").select('DRVR_ZIP').groupBy('DRVR_ZIP').count()
        return tempDataframe.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn<=5").select('DRVR_ZIP')
        






