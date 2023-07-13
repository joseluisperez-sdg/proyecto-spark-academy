class Transformation:
    def __init__(self, name, type, input, params):
        self.name = name
        self.type = type
        self.input = input
        params.pop("input")
        self.params = params