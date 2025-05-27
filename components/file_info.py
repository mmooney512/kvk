class file_info:
    def __init__(self, 
                 file_path,
                 file_schema, 
                 output_table_name, 
                 file_header="true", 
                 primary_key=None, 
                 file_type="csv", 
                 file_sep=",") -> None:
        
        
        self.file_path = file_path
        self.file_schema = file_schema
        self.output_table_name = output_table_name
        self.file_header = file_header
        self.file_type = file_type
        self.file_sep = file_sep
        self.primary_key = primary_key
    def check_locale():
        pass
    