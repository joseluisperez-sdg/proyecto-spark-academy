import os
import shutil


class LoadService:
    """
        Clase que representa el componente de Carga de la ETL realizada.

        Attributes
        ----------
        _spark_session : SparkSession
            sesión de spark
        _logger : LoggerAgent
            objeto logger usado en el programa.

        Methods
        -------
        load_correct(self, data_containers, sinks, index_containers):
            para cada uno de los Sinks que no son de error se ejecuta la escritura con la información proporcionada.

        load_errors(self, data_containers, sinks, index_containers):
            escritura de los df_error con los datos proporcionados por el Sink de error.
    """

    def __init__(self, spark_session, logger):
        self._spark_session = spark_session
        self._logger = logger

    def load_correct(self, data_containers, sinks, index_containers):
        """
            Para cada uno de los Sinks que no son de error se ejecuta la escritura con la información proporcionada.

            Parameters:
                data_containers (list[DataContainers]): DataContainers con los datos necesarios a guardar.
                sinks (list[Sinks]): Sinks específicos del Dataflow.
                index_containers (dict): índice de DataContainers.
        """

        self._logger.info_start()

        for sink in sinks:
            if sink.name != "raw-ko":
                if sink.input not in index_containers.keys(): raise Exception(f"{sink.name} require {sink.input} container, not found in index: {index_containers}")
                df_to_save = data_containers[index_containers[sink.input]].df_ok
                for path in sink.paths:
                    if sink.options is not None:
                        df_to_save.coalesce(1).write.format(sink.file_format).options(**sink.options) \
                        .mode(sink.save_mode).save(path + "tmp")
                    else:
                        df_to_save.coalesce(1).write.format(sink.file_format) \
                        .mode(sink.save_mode).save(path + "tmp")

                    part_filename = next(entry for entry in os.listdir(path + "tmp") if entry.startswith('part-'))
                    temporary_json = os.path.join(path + "tmp", part_filename)
                    shutil.copyfile(temporary_json, path + sink.name + "." + sink.file_format.lower())
                    shutil.rmtree(path + "tmp")

        self._logger.info_finish()

    def load_errors(self, data_containers, sinks, index_sinks):
        """
            Escritura de los df_error con los datos proporcionados por el Sink de error.

            Parameters:
                data_containers (list[DataContainers]): DataContainers con los datos necesarios a guardar.
                sinks (list[Sinks]): Sinks específicos del Dataflow.
                index_sinks (dict): índice de Sinks.
        """

        self._logger.info_start()

        if "raw-ko" not in index_sinks.keys(): raise Exception("Mandatory to have an error sink")
        error_sink = sinks[index_sinks["raw-ko"]]

        for i, data_container in enumerate(data_containers):
            if data_container.df_error is not None:
                if error_sink.options is not None:
                    data_container.df_error.coalesce(1).write.format(error_sink.file_format).options(**error_sink.options) \
                    .mode(error_sink.save_mode).save(error_sink.paths[i] + "tmp")
                else:
                    data_container.df_error.coalesce(1).write.format(error_sink.file_format)\
                        .mode(error_sink.save_mode).save(error_sink.paths[i] + "tmp")

                part_filename = next(entry for entry in os.listdir(error_sink.paths[i] + "tmp") if entry.startswith('part-'))
                temporary_json = os.path.join(error_sink.paths[i] + "tmp", part_filename)
                shutil.copyfile(temporary_json, error_sink.paths[i] + error_sink.name + "." + error_sink.file_format.lower())
                shutil.rmtree(error_sink.paths[i] + "tmp")

        self._logger.info_finish()


