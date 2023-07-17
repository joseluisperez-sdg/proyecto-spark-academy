"""
logging
~~~~~~~
Este módulo contiene una clase que envuelve el objeto log4j instanciado
por el SparkContext activo, habilitando Log4j logging para el uso de PySpark.
"""
import logging
class Log4j(object):
    """Clase Wrapper para el objeto Log4j JVM.

        Parameters:
            spark: SparkSession object.
    """

    def __init__(self, spark):
        self.logger = logging.getLogger("py4j")
        self.logger.setLevel(logging.INFO)
        # self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log error.

        Parameters:
            message (str): Mensaje de error que escribir a log
        Returns:
            None
        """
        self.logger.error(message)
        return None

    def warning(self, message):
        """Log warning.

        Parameters:
            message (str): Mensaje de error que escribir a log
        Returns:
            None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        Parameters:
            message (str): Mensaje de información que escribir a log
        Returns:
            None
        """
        self.logger.info(message)
        return None

    def debug(self, message):
        """Log debug.

        Parameters:
            message (str): Mensaje de depuración que escribir a log
        Returns:
            None
        """
        self.logger.debug(message)
        return None

    def critical(self, message):
        """Log critical.

        Parameters:
            message (str): Mensaje critico que escribir a log
        Returns:
            None
        """
        self.logger.critical(message)
        return None

    def exception(self, message):
        """Log exception.

        Parameters:
            message (str): Mensaje de excepción que escribir a log
        Returns:
            None
        """
        self.logger.exception(message)
        return None
