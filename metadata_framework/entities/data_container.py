class DataContainer:
    def __init__(self, name, path, file_format):
        self.original_name = name
        self.name = name
        self.error_name = None
        self.path = path
        self.file_format = file_format
        self.df_ok = None
        self.df_error = None
