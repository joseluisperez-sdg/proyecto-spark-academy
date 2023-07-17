class DataContainer:
    """
        Clase que representa un contenedor de datos.

        Attributes
        ----------
        name : str
            nombre del DataContainer
        status_name : str
            nombre usado para controlar las validaciones.
        error_name : str
            nombre usado para controlar las validaciones, validaciones_ko.
        path : str
            ruta al fichero de datos.
        file_format : str
            formato del fichero de datos de entrada.
        options : dict
            diccionario con opciones adicionales de lectura permitidas por Spark.
        df_ok : Dataframe
            dataframe con los datos del fichero fuente que han pasado las validaciones.
        df_error : Dataframe
            dataframe con los datos del fichero fuente que no han pasado las validaciones.
    """
    def __init__(self, name, path, file_format, options = None):
        self.name = name
        self.status_name = name
        self.error_name = None
        self.path = path
        self.file_format = file_format
        self.options = options
        self.df_ok = None
        self.df_error = None
