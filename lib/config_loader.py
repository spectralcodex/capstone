import configparser

from pyspark import SparkConf


class ConfigLoader:

    @staticmethod
    def get_app_config(env: str) -> dict:
        config = configparser.ConfigParser()
        config.read('conf/sbdl.conf')
        conf = {}
        for (k, v) in config.items(env):
            conf.update({k, v})
        return conf

    @staticmethod
    def get_spark_config(env: str) -> SparkConf:
        spark_conf = SparkConf()
        config = configparser.ConfigParser()
        config.read('conf/spark.conf')

        for (k, v) in config.items(env):
            spark_conf.set(k, v)
        return spark_conf

    @staticmethod
    def get_conf_filter(env, param):
        conf = ConfigLoader.get_app_config(env)
        return 'true' if conf[param] == '' else conf[param]
