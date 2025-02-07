from dbms.file_manager import DatabaseFileManager
import pickle


class Database:
    def __init__(self, db_name:str):
        self.db_name = db_name
        self.file_manager = DatabaseFileManager(db_name)
        self.file_path: str = self.file_manager.file_path
        self.metadata = self.load_metadata(self.file_manager.file_path + "metadata")
    
    def save_metadata(self ,metadata:dict) -> None:
        """
        Overrides the metadata to the metadata file.

        Parameters:
            metadata (dict): The metadata to save.
        """
        filename = self.file_path + "metadata"
        with open(filename, "wb") as file:
            pickle.dump(metadata, file)

    def load_metadata(self, filename: str) -> dict:
        """
        Loads the metadata from the specified file.

        Parameters:
            filename (str): The path to the metadata file.

        Returns:
            dict: The metadata of the database.
        """
        try:
            with open(filename, "rb") as file:
                return pickle.load(file)
        except FileNotFoundError:
            raise FileNotFoundError("Metadata file not found")

    def create_table(self, table_name: str, columns:list, data_types:list, primary_key:list, foreing_keys:list) -> None:
        '''
        Creates a new table in the database.
        
        Parameters:
            table_name (str): The name of the table.
            columns (list): The list of column names.
            data_types (list): The list of data types for the columns.
            primary_key (str): The primary key column.
            foreign_keys (dict): The foreign keys for the table.
        '''


        if table_name in self.metadata.keys():
            raise ValueError("Table already exists")
        
        metadata_table = {
            "columns": columns,
            "data_types": data_types,
            "primary_key": primary_key,
            "foreign_keys": foreing_keys or {}
        }

        self.metadata[table_name] = metadata_table
        self.save_metadata(self.metadata)
        self.file_manager.create_csv(table_name)

    def drop_table(self, table_name:str) -> None:
        '''
        Drops a table from the database.

        Parameters:
            table_name (str): The name of the table.
        '''
        if table_name not in self.metadata.keys():
            raise ValueError("Table not found")
        
        del self.metadata[table_name]
        self.save_metadata(self.metadata)
        self.file_manager.drop_csv(table_name)
    
    def get_info_table(self, table_name:str) -> dict:
        '''
        Returns the metadata of the table.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            dict: The metadata of the table.
        '''
        if table_name not in self.metadata.keys():
            raise ValueError("Table not found")
        
        return self.metadata[table_name]
    
    
