class Sink():
    def __init__(self, name, input, paths, file_format, save_mode, options = None):
        self.name = name
        self.input = input
        self.paths = paths
        self.file_format = file_format
        self.save_mode = save_mode
        self.options = options

