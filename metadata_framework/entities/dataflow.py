class Dataflow:
    def __init__(self, name, data_containers, transformations, sinks):
        self.name = name
        self.data_containers = data_containers
        self.transformations = transformations
        self.sinks = sinks
        self.index_containers = self._generate_index(data_containers)
        self.index_sinks = self._generate_index(sinks)

    def _generate_index(self, data):
        index = {}
        for i, container in enumerate(data):
            index[container.name] = i

        return index



