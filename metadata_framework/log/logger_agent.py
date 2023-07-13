'''Module logger_agent implement the class LoggerAgent'''
import inspect

from pyspark.sql import SparkSession

from metadata_framework.log import log


class LoggerAgent():
    '''LoggerAgent: This class is the agent to managed the Logs'''

    def __init__(self, spark_logger: log.Log4j):
        self.spark_logger = spark_logger

    @staticmethod
    def init_logger(spark_session: SparkSession):
        '''Initialization of the LoggerAgent
        path, filename, log_level are optional.
        '''
        return LoggerAgent(log.Log4j(spark_session))

    def debug(self, message: str):
        '''Method to log a standard message to the log with debug level'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.debug(long_message)

    def info(self, message: str):
        '''Method to log a standard message to the log with info level'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def info_start(self, message: str = ''):
        '''Method to log a standard message to the log with info start level'''
        long_message = '%s start %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def info_finish(self, message: str = ''):
        '''Method to log a standard message to the log with info finish level'''
        long_message = '%s finish %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def warning(self, message: str):
        '''Method to log a standard message to the log with warning level'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.warning(long_message)

    def error(self, message: str):
        '''Method to log a standard message to the log with error level'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.error(long_message)

    def critical(self, message: str):
        """critical method

        Args:
            message (str): message to write on log4j
        """
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.critical(long_message)

    def exception(self, message: str):
        """exception method

        Args:
            message (str): message to write on log4j
        """
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.exception(long_message)

