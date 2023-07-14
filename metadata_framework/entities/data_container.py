class DataContainer:
    def __init__(self, name, path, file_format, options = None):
        self.name = name
        self.status_name = name
        self.error_name = None
        self.path = path
        self.file_format = file_format
        self.options = options
        self.df_ok = None
        self.df_error = None
