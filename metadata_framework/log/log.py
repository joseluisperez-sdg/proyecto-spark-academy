"""
logging
~~~~~~~

This module contains a class that wraps the log4j object instantiated
by the active SparkContext, enabling Log4j logging for PySpark using.
"""
import logging
class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        # conf = spark.sparkContext.getConf()
        # app_id = conf.get('spark.app.id')
        # app_name = conf.get('spark.app.name')

        # log4j = spark._jvm.org.apache.log4j
        # message_prefix = '<' + app_name + ' ' + app_id + '>'
        # message_prefix = "<bloqueo_borrado>"
        self.logger = logging.getLogger("py4j")
        self.logger.setLevel(logging.INFO)
        # self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warning(self, message):
        """Log warning.

        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None

    def debug(self, message):
        """Log debug.

        :param: debug message to write to log
        :return: None
        """
        self.logger.debug(message)
        return None

    def critical(self, message):
        """Log critical.

        :param: Critical message to write to log
        :return: None
        """
        self.logger.critical(message)
        return None

    def exception(self, message):
        """Log exception.

        :param: Exception message to write to log
        :return: None
        """
        self.logger.exception(message)
        return None
