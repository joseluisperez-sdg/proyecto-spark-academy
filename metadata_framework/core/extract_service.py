class ExtractService:

    def __init__(self, spark_session, logger):
        self._spark_session = spark_session
        self._logger = logger

    def extract_source(self, data_containers):
        self._logger.info_start()

        for i, data_container in enumerate(data_containers):
            data_container.df_ok = self._spark_session.read.format(data_container.file_format).load(data_container.path)
            if "_corrupt_record" in data_container.df_ok.columns: raise Exception(f"Data source {i} has corrupted records")

        self._logger.info_finish()


