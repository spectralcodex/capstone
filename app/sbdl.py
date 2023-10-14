from lib.logger import Log4j
from lib import utils


class SBDL:

    def __init__(self, env):
        self.env = env
        self.logger = None
        self.spark = None

    def get_session(self):
        try:
            self.spark = Utils.get_spark_session(self.env)
            self.logger = Log4j(self.spark)
        except Exception as e:
            print('spark session creation error: ', e)

    def run(self):
        self.get_session()

        if not self.spark:
            print('Session could not initialized!!!')
            return
        self.logger.info('Session created!')
