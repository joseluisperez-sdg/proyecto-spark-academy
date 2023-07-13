from pyspark.sql import SparkSession
from multiprocessing.pool import ThreadPool
import argparse

from metadata_framework.entities.generate import generate_dataflows
from metadata_framework.core.extract_service import ExtractService
from metadata_framework.core.transform_service import TransformService
from metadata_framework.core.load_service import LoadService

from metadata_framework.log.logger_agent import LoggerAgent


def etl(params):
    dataflow_id = params["dataflow_id"]
    try:
        params["extractor"].extract_source(params["dataflow"].data_containers)

        # params["dataflow"].data_containers[0].df.show()
        # print(params["dataflow"].index)

        params["transformer"].perform_validations(params["dataflow"].data_containers, params["dataflow"].index_containers, params["dataflow"].transformations)
        params["transformer"].perform_functions(params["dataflow"].data_containers, params["dataflow"].index_containers, params["dataflow"].transformations)

        # print(params["dataflow"].data_containers[0].name)
        # params["dataflow"].data_containers[0].df_ok.show()
        # print(params["dataflow"].data_containers[0].error_name)
        # params["dataflow"].data_containers[0].df_error.show()
        # print(params["dataflow"].index_containers)
        # print(params["dataflow"].index_sinks)

        params["loader"].load_correct(params["dataflow"].data_containers, params["dataflow"].sinks, params["dataflow"].index_containers)
        params["loader"].load_errors(params["dataflow"].data_containers, params["dataflow"].sinks, params["dataflow"].index_sinks)

        return 1
    except Exception as error:
        print(f"Dataflow {dataflow_id} has an error: {error}")
        return 0


def parallelize(processes_data, n_processes):
    pool = ThreadPool(n_processes)
    status = pool.map(etl, [process_data for process_data in processes_data])
    if all(status):
        print("OK")
    else:
        print("Error")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_processes", required=True)
    args = parser.parse_args()

    n_processes = int(args.n_processes)

    if n_processes <= 0: raise Exception("Param n_processes must be greater than zero")

    jar_packages = []
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder = (SparkSession.builder.appName("metadata_framework"))
    spark_builder.config('spark.jars.packages', spark_jars_packages)
    spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    logger = LoggerAgent.init_logger(spark_session)

    dataflows = generate_dataflows(logger)

    extractor = ExtractService(spark_session, logger)
    transformer = TransformService(spark_session, logger)
    loader = LoadService(spark_session, logger)

    processes_data = [
        {"dataflow_id": i,
         "dataflow": dataflow,
         "extractor": extractor,
         "transformer": transformer,
         "loader": loader} for i, dataflow in enumerate(dataflows)]

    parallelize(processes_data, n_processes)


if __name__ == '__main__':
    main()

