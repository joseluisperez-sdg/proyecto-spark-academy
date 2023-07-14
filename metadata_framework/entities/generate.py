from metadata_framework.entities.data_container import DataContainer
from metadata_framework.entities.transformation import Transformation
from metadata_framework.entities.sink import Sink
from metadata_framework.entities.dataflow import Dataflow
import json


def generate_data(source_info):
    required_keys = {'name', 'path', 'format'}
    if not required_keys.issubset(set(source_info.keys())): raise Exception(f"Mandatory keys in sources: {required_keys}")
    if "options" in source_info.keys():
        return DataContainer(source_info["name"],source_info["path"],source_info["format"], source_info["options"])
    else:
        return DataContainer(source_info["name"], source_info["path"], source_info["format"])


def generate_transformation(transformation_info):
    required_keys = {"name", "type", "params"}
    if not required_keys.issubset(set(transformation_info.keys())): raise Exception(f"Mandatory keys in validations and transformations: {required_keys}")
    return Transformation(transformation_info["name"], transformation_info["type"], transformation_info["params"]["input"], transformation_info["params"])


def generate_sink(sink_info):
    required_keys = {"name", "input", "paths", "format", "saveMode"}
    if not required_keys.issubset(set(sink_info.keys())): raise Exception(f"Mandatory keys in sinks: {required_keys}")
    if "options" in sink_info.keys():
        return Sink(sink_info["name"],sink_info["input"],sink_info["paths"], sink_info["format"],sink_info["saveMode"],sink_info["options"])
    else:
        return Sink(sink_info["name"], sink_info["input"], sink_info["paths"], sink_info["format"],sink_info["saveMode"])


def generate_dataflows(logger):
    logger.info_start()

    metadata_path = 'data/metadata.json'
    f = open(metadata_path)
    metadata_info = json.load(f)
    if len(metadata_info['dataflows']) <= 0: raise Exception(f"No dataflows provided in file {metadata_path}")
    dataflows = []

    for dataflow in metadata_info['dataflows']:
        name = dataflow["name"]
        if len(dataflow['sources']) <= 0: raise Exception(f"No sources provided in file {metadata_path} for dataflow {name}")
        data_containers = [generate_data(source) for source in dataflow["sources"]]
        if len(dataflow['transformations']) <= 0: raise Exception(f"No sources provided in file {metadata_path} for dataflow {name}")
        transformations = [generate_transformation(transformation) for transformation in dataflow["transformations"]]
        if len(dataflow['sinks']) <= 0: raise Exception(f"No sources provided in file {metadata_path} for dataflow {name}")
        sinks = [generate_sink(sink) for sink in dataflow["sinks"]]

        dataflows.append(Dataflow(name, data_containers, transformations, sinks))

    logger.info_finish()

    return dataflows




