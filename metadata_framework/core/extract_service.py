class ExtractService:
    """
        Clase que representa el componente de Extracción de la ETL realizada.

        Attributes
        ----------
        _spark_session : SparkSession
            sesión de spark
        _logger : LoggerAgent
            objeto logger usado en el programa.

        Methods
        -------
        _extract_source(self, data_containers):
            lectura de los datos de los ficheros fuente especificados. El Dataframe resultante se guarda en la variable df_ok del DataContainer.
    """

    def __init__(self, spark_session, logger):
        self._spark_session = spark_session
        self._logger = logger

    def extract_source(self, data_containers):
        """
            Método de la lectura de los datos de los ficheros fuente especificados. El Dataframe resultante se guarda en la variable df_ok del DataContainer.

            Parameters:
                    data_containers (list[DataContainer]): lista de los DataContainers del Dataflow en ejecución
        """
        self._logger.info_start()

        for i, data_container in enumerate(data_containers):
            if data_container.options is not None:
                data_container.df_ok = self._spark_session.read.format(data_container.file_format).options(**data_container.options).load(data_container.path)
            else:
                data_container.df_ok = self._spark_session.read.format(data_container.file_format).load(
                    data_container.path)
            if "_corrupt_record" in data_container.df_ok.columns: raise Exception(f"Data source {i} has corrupted records")

        self._logger.info_finish()


