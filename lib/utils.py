from pyspark.sql import SparkSession

from lib.config_loader import ConfigLoader


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=ConfigLoader.get_spark_config(env)) \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()


