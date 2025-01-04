import os
import sys

from pyspark import SparkConf
from pyspark.shell import spark
from pyspark.sql import *
from lib.logger import Log4J
from p3.lib.utils import get_spark_app_config, load_survey_df

#local multithreaded JVM with 4 threads    = local[4] below
#app_name in logger.py goes from appName set here


if __name__ == "__main__":
    #obtaining a spark session and setting configurations
    '''spark = SparkSession.builder\
            .appName("Hello Spark")\
            .master("local[3]")\
            .getOrCreate()'''

    #Another way for configuring spark session
    config = SparkConf()
    config.set("spark.app.name","Hello Spark4")
    config.set("spark.master","local[4]")
    spark = SparkSession.builder\
            .config(conf=config)\
            .getOrCreate()

    #3rd way for configuring spark session
    '''config1= get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=config1) \
        .getOrCreate()'''

    logger = Log4J(spark)
    logger.info("Starting hellospark")


    # if for checking cmd line arg, if cmd line arg is not provided err is logged
    ''' if len(sys.argv) != 2:
        logger.error("UsageHelloSpark <filename>")
        sys.exit(-1)
    '''
    # Validate file existence
    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        logger.error(f"Input file '{input_file}' does not exist or is not accessible.")
        sys.exit(-1)
    # spark.conf.set("spark.debug.maxToStringFields", 10000)
    survey_df = load_survey_df(spark, sys.argv[1])
    survey_df.show()

    '''
    #Reading all spark configs
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    '''


    logger.info("Finished hellospark")
    spark.stop()
