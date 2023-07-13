from pyspark.sql.functions import expr, lit, col, array, when, array_union, size, current_timestamp
import re


class TransformService:

    def __init__(self, spark_session, logger):
        self._spark_session = spark_session
        self._logger = logger
        self._transformation_catalog = {
            "notNull": "field1 is NOT NULL",
            "notEmpty": "field1 != \"\"",
            "current_timestamp": "CURRENT_TIMESTAMP"
        }

    def _process_fields(self, transform_expression, fields, additional = None):

        filled_expression = transform_expression
        if isinstance(fields, list):
            for field in fields:
                filled_expression = re.sub("field([0-9]+)", field, filled_expression, count=1)

            if additional is not None:
                filled_expression = filled_expression.replace("additional",additional)
        else:
            filled_expression = filled_expression.replace("field1", fields)

        return filled_expression

    def perform_validations(self, data_containers, index, transformations):

        self._logger.info_start()

        for transformation in transformations:
            if transformation.type == "validate_fields":
                input = transformation.input
                if input not in index.keys(): raise Exception(f"{input} not found in data_containers index: {index}")
                data_container_input = data_containers[index[input]]
                input_df_cols = data_container_input.df_ok.columns
                data_container_input.df_ok = data_container_input.df_ok.withColumn("arraycoderrorbyfield", array())

                for validation in transformation.params["validations"]:
                    field = validation["field"]
                    for field_validation in validation["validations"]:
                        if field_validation not in self._transformation_catalog.keys(): assert Exception(f"Validation {field_validation} is not in the catalog, please add it")
                        catalog_validation = self._process_fields(self._transformation_catalog[field_validation], field)
                        data_container_input.df_ok = data_container_input.df_ok.withColumn("arraycoderrorbyfield", when(expr(catalog_validation), col("arraycoderrorbyfield")).
                                                                                               otherwise(array_union(col("arraycoderrorbyfield"), array(lit(f"{field}: error validation {field_validation}")))))

                data_container_input.df_error = data_container_input.df_ok.filter(size(col("arraycoderrorbyfield")) > 0).select(*input_df_cols, current_timestamp().alias("dt"), "arraycoderrorbyfield")
                data_container_input.df_ok = data_container_input.df_ok.filter(size(col("arraycoderrorbyfield")) == 0).drop("arraycoderrorbyfield")

                id = index[input]
                data_container_input.name = f"validation_ok_{id}"
                data_container_input.error_name = f"validation_ko_{id}"

                index[data_container_input.name] = id
                index.pop(input)

        self._logger.info_finish()

    def perform_functions(self, data_containers, index, transformations):

        self._logger.info_start()

        for data_container_input in data_containers:
            if "validation_ok" in data_container_input.name:
                id = index[data_container_input.name]
                validation_name = data_container_input.name

                for transformation in transformations:
                    if transformation.type != "validate_fields":
                        function_key = list(transformation.params.keys())[0]
                        for function_params in transformation.params[function_key]:
                            if "fields" in function_params.keys():
                                additional = function_params["additional"] if "additional" in function_params else None
                                function = function_params["function"]
                                if function_params["function"] not in self._transformation_catalog.keys(): assert Exception(f"Function {function} is not in the catalog, please add it")
                                catalog_function = self._process_fields(self._transformation_catalog[function], function_params["fields"], additional)
                            else:
                                catalog_function = self._transformation_catalog[function_params["function"]]
                            data_container_input.df_ok = data_container_input.df_ok.select("*", expr(catalog_function).alias(function_params["name"]))

                        data_container_input.name = transformation.name

                index[data_container_input.original_name] = id
                index.pop(validation_name)

        self._logger.info_finish()









