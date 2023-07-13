import os
import shutil


class LoadService:

    def __init__(self, spark_session, logger):
        self._spark_session = spark_session
        self._logger = logger

    def load_correct(self, data_containers, sinks, index_containers):

        self._logger.info_start()

        for sink in sinks:
            if sink.name != "raw-ko":
                if sink.input not in index_containers.keys(): raise Exception(f"{sink.name} require {sink.input} container, not found in index: {index_containers}")
                df_to_save = data_containers[index_containers[sink.input]].df_ok
                for path in sink.paths:
                    df_to_save.coalesce(1).write.format(sink.file_format).option("ignoreNullFields", False) \
                        .mode(sink.save_mode).save(path + "tmp")

                    part_filename = next(entry for entry in os.listdir(path + "tmp") if entry.startswith('part-'))
                    temporary_json = os.path.join(path + "tmp", part_filename)
                    shutil.copyfile(temporary_json, path + sink.name + "." + sink.file_format.lower())
                    shutil.rmtree(path + "tmp")

        self._logger.info_finish()

    def load_errors(self, data_containers, sinks, index_sinks):

        self._logger.info_start()

        if "raw-ko" not in index_sinks.keys(): raise Exception("Mandatory to have an error sink")
        error_sink = sinks[index_sinks["raw-ko"]]

        for i, data_container in enumerate(data_containers):
            if data_container.df_error is not None:
                data_container.df_error.coalesce(1).write.format(error_sink.file_format).option("ignoreNullFields", False) \
                    .mode(error_sink.save_mode).save(error_sink.paths[i] + "tmp")

                part_filename = next(entry for entry in os.listdir(error_sink.paths[i] + "tmp") if entry.startswith('part-'))
                temporary_json = os.path.join(error_sink.paths[i] + "tmp", part_filename)
                shutil.copyfile(temporary_json, error_sink.paths[i] + error_sink.name + "." + error_sink.file_format.lower())
                shutil.rmtree(error_sink.paths[i] + "tmp")

        self._logger.info_finish()


