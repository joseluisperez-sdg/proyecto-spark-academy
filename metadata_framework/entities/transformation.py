class Transformation:
    """
        Clase que representa un contenedor de datos.

        Attributes
        ----------
        name : str
            nombre de la Transformation.
        type : str
            tipo de la Transformation.
        input : str
            DataContainer de entrada que necesita.
        params : dict
            parámetros específicos de la validación o transformación.
    """
    def __init__(self, name, type, input, params):
        self.name = name
        self.type = type
        self.input = input
        params.pop("input")
        self.params = params