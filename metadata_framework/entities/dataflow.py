class Dataflow:
    """
        Clase que representa un Dataflow

        Attributes
        ----------
        name : str
            nombre del Dataflow
        data_containers : list[DataContainers]
            lista de contenedores de datos del Dataflow.
        transformations : list[Transformations]
            lista de transformaciones del Dataflow.
        sinks : list[Sinks]
            lista de salidas del Dataflow.
        index_containers : dict
            diccionario de tipo clave-valor que introduciendo el nombre del DataContainer como clave devuelve su índice en la lista del Dataflow.
        index_sinks : dict
            diccionario de tipo clave-valor que introduciendo el nombre del Sink como clave devuelve su índice en la lista del Dataflow.

        Methods
        -------
        _generate_index(self, data):
            genera el indice de la lista pasada por parámetro.
    """

    def __init__(self, name, data_containers, transformations, sinks):
        self.name = name
        self.data_containers = data_containers
        self.transformations = transformations
        self.sinks = sinks
        self.index_containers = self._generate_index(data_containers)
        self.index_sinks = self._generate_index(sinks)

    def _generate_index(self, data):
        """
            Método encargado de generar el índice de la lista pasada.

            Parameters:
                    data (list): lista con objetos.
            Returns:
                    index (dict): indice clave-valor.
        """
        index = {}
        for i, container in enumerate(data):
            index[container.name] = i

        return index



