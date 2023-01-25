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
from typing import *


class Jobs:
    def __init__(self):
        pass
    
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass
        
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
        

    def complexVEHMake(self,**kwargs)->DataFrame:
        """_summary_
            Determine the Top 5 Vehicle Makes based on:
                1. Where drivers are charged with speeding related offences
                2. Has licensed Drivers
                3. Used top 10 used vehicle colours
                4. Has car licensed with the Top 25 states with highest number of offences 
        Args:
            chargesDataframe (DataFrame): Pyspark DataFrame object.
            unitsDataframe (DataFrame): Pyspark DataFrame object.
            personDataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        try:
            chargeSpeed = kwargs.get('chargesDataframe').filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','UNIT_NBR','CHARGE') #charge corresponding to speed issues
            topClrsAgg = kwargs.get('unitsDataframe').filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').count()
            topClrsDataframe = topClrsAgg.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=10").select('VEH_COLOR_ID')#top10 vehicle colors used
            topStatesAgg = kwargs.get('unitsDataframe').filter(~col('VEH_LIC_STATE_ID').isin(settings.EXCLUDED_STATES)).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').count()
            topStatesDataframe = topStatesAgg.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=25").select('VEH_LIC_STATE_ID')
            unitSubJoin = kwargs.get('unitsDataframe').join(topClrsDataframe,on=['VEH_COLOR_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
            chargeSubJoin = unitSubJoin.join(chargeSpeed,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
            combineJoin = chargeSubJoin.join(topStatesDataframe,on=['VEH_LIC_STATE_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')
            licensedPersonDataframe = kwargs.get('personDataframe').filter("DRVR_LIC_CLS_ID != 'UNLICENSED'").select('CRASH_ID','UNIT_NBR')
            finalTempDataframe = combineJoin.join(licensedPersonDataframe,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','VEH_MAKE_ID').groupBy('VEH_MAKE_ID').count()   
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n"+"Args:: chargesDataframe:DataFrame, unitsDataframe:DataFrame, personDataframe:DataFrame")
        finally:
            return finalTempDataframe.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <= 5").select('VEH_MAKE_ID')
    
    def noDamageProperty(self,**kwargs)-> DataFrame:
        """_summary_
            Get Unique Property where:
                1. No Damaged Property was observed.
                2. Damage Level (VEH_DMAG_SCL~) is above 4.
                3. Car is Insured.
        Args:
            chargesDataframe (DataFrame): Pyspark DataFrame object.
            damagesDataframe (DataFrame): Pyspark DataFrame object.
            unitsDataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        try:
            noInsuranceDataframe = kwargs.get('chargesDataframe').filter(col('CHARGE').contains('NO')).filter(col('CHARGE').contains('INSURANCE')).select('CRASH_ID','UNIT_NBR').withColumnRenamed('CRASH_ID','I_CRASH_ID').withColumnRenamed('UNIT_NBR','I_UNIT_NBR')
            damagedPropertyDataframe = kwargs.get('damagesDataframe').select('CRASH_ID').distinct().withColumnRenamed('CRASH_ID','D_CRASH_ID')
            combinedDataframe= kwargs.get('unitsDataframe').join(noInsuranceDataframe,(kwargs.get('unitsDataframe')['CRASH_ID']==noInsuranceDataframe['I_CRASH_ID']) & (kwargs.get('unitsDataframe')['UNIT_NBR']==noInsuranceDataframe['I_UNIT_NBR']),how='left')
            unitsCombined = combinedDataframe.filter("I_CRASH_ID is null").select('CRASH_ID','UNIT_NBR','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
            damagedCombined = unitsCombined.join(damagedPropertyDataframe,unitsCombined['CRASH_ID']==damagedPropertyDataframe['D_CRASH_ID'],how='left')
            lvlCombined= damagedCombined.filter("D_CRASH_ID is null").select('CRASH_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID') 
            resultDataframe = lvlCombined.withColumn('DMAG1_RANGE',regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
	                        .withColumn('DMAG2_RANGE',regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
	                        .filter("DMAG1_RANGE > 4 or DMAG2_RANGE > 4") \
	                        .select('CRASH_ID').distinct().count()
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n"+"Args:: chargesDataframe:DataFrame, damagesDataframe:DataFrame, unitsDataframe:DataFrame")
        finally:
            return resultDataframe
    
    def bodyStyleWiseCrash(self,Dataframe:DataFrame,**kwargs)-> DataFrame:
        """_summary_
            Get top 5 body style with the top ethnic user group.       
        Args:
            unitsDataframe (DataFrame): Pyspark DataFrame object.
            personDataframe (DataFrame): Pyspark DataFrame object.
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        # removing rows where person ethnicity id is not registered
        try:
            # removing rows where person ethnicity id is not registered
            ethnicDataframe = kwargs.get('personDataframe').filter("PRSN_ETHNICITY_ID != 'NA' and PRSN_ETHNICITY_ID != 'UNKNOWN' and PRSN_ETHNICITY_ID != 'OTHER'").select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID')
            # removing rows where vehicle body style id is not registered
            bodyStyleDataframe = kwargs.get('unitsDataframe').filter("VEH_BODY_STYL_ID != 'NA' and VEH_BODY_STYL_ID != 'UNKNOWN' and VEH_BODY_STYL_ID != 'NOT REPORTED'").filter(~col('VEH_BODY_STYL_ID').like('OTHER%')).select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID')
            combinedDataframe = bodyStyleDataframe.join(ethnicDataframe,on=['CRASH_ID','UNIT_NBR'],how='inner')
            bodyStyleEthnicDataframe = combinedDataframe.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count() 
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n"+"Args:: unitsDataframe:DataFrame, personDataframe:DataFrame")
        finally:
            return bodyStyleEthnicDataframe.withColumn('rn',rank().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc()))).filter("rn == 1").select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')
        