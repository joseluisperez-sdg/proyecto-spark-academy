class Sink():
    """
        Clase que representa una salida de datos.

        Attributes
        ----------
        name : str
            nombre del Sink.
        input : str
            DataContainer de entrada que necesita.
        paths : list(str)
            rutas de salida.
        file_format : str
            formato del fichero de datos de salida.
        save_mode : str
            modo de guardado.
        options : dict
            diccionario con opciones adicionales de escritura permitidas por Spark.
    """
    def __init__(self, name, input, paths, file_format, save_mode, options = None):
        self.name = name
        self.input = input
        self.paths = paths
        self.file_format = file_format
        self.save_mode = save_mode
        self.options = options

