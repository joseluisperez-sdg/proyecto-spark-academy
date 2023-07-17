'''Módulo logger_agent implementa la clase LoggerAgent'''
import inspect

from pyspark.sql import SparkSession

from metadata_framework.log import log


class LoggerAgent():
    '''LoggerAgent: Esta clase es el agente que gestiona los Logs'''

    def __init__(self, spark_logger: log.Log4j):
        self.spark_logger = spark_logger

    @staticmethod
    def init_logger(spark_session: SparkSession):
        '''Inicialización del LoggerAgent'''
        return LoggerAgent(log.Log4j(spark_session))

    def debug(self, message: str):
        '''Método para registrar un mensaje estándar en el registro con nivel de depuración'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.debug(long_message)

    def info(self, message: str):
        '''Método para registrar un mensaje estándar en el registro con nivel info'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def info_start(self, message: str = ''):
        '''Método para registrar un mensaje estándar en el registro con información de inicio'''
        long_message = '%s start %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def info_finish(self, message: str = ''):
        '''Método para registrar un mensaje estándar en el registro con información de finalización'''
        long_message = '%s finish %s' % (inspect.stack()[1][3], message)
        self.spark_logger.info(long_message)

    def warning(self, message: str):
        '''Método para registrar un mensaje estándar en el registro con nivel de advertencia'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.warning(long_message)

    def error(self, message: str):
        '''Método para registrar un mensaje estándar en el registro con nivel de error'''
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.error(long_message)

    def critical(self, message: str):
        """critical method

        Args:
            message (str): Mensaje para escribir en log4j
        """
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.critical(long_message)

    def exception(self, message: str):
        """exception method

        Args:
            message (str): Mensaje para escribir en log4j
        """
        long_message = '%s said %s' % (inspect.stack()[1][3], message)
        self.spark_logger.exception(long_message)

