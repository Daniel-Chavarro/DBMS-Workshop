import os
import pickle
from dbms.file_manager import DatabaseFileManager
from dbms.table import Table
from dbms.exeptions import DroppedDatabaseError



class Database:
    '''
    Represents a database.
    
    Attributes:
        db_name (str): The name of the database.
        file_manager (DatabaseFileManager): The file manager for the database.
        file_path (str): The path to the database file.
        metadata (dict): The metadata of the database.
        tables (dict): The tables in the database.
    '''
    
    def __init__(self, db_name:str):
        '''
        Initializes the database.
        
        Parameters:
            db_name (str): The name of the database.
        '''
        self.db_name = db_name
        self.file_manager = DatabaseFileManager(self)
        self.file_path: str = self.file_manager.file_path
        self.metadata = self.load_metadata(self.file_manager.file_path + "metadata")
        self.tables = {name: Table(name, self) for name in self.metadata.keys()}

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

    def create_table(self, table_name: str, columns:list, data_types:list , primary_key:list, foreing_keys) -> None:
        """
        Creates a new table in the database.

        Parameters:
            table_name (str): The name of the table.
            columns (list): The list of column names.
            data_types (list): The list of data types for the columns.
            primary_key (str): The primary key column.
            foreign_keys (dict): The foreign keys for the table.
        """


        if table_name in self.metadata.keys():
            raise ValueError("Table already exists")
        
        metadata_table = {
            "columns": columns,
            "data_types": data_types,
            "primary_key": primary_key,
            "foreign_keys": foreing_keys
        }

        self.metadata[table_name] = metadata_table
        self.tables[table_name] = Table(table_name, self)
        self.save_metadata(self.metadata)
        self.file_manager.create_csv(table_name)
        print(f"Table {table_name} created")

    def drop_table(self, table_name:str) -> None:
        '''
        Drops a table from the database.

        Parameters:
            table_name (str): The name of the table.
        '''
        if table_name not in self.metadata.keys():
            raise ValueError("Table not found")
        
        del self.metadata[table_name]
        del self.tables[table_name]
        self.save_metadata(self.metadata)
        self.file_manager.drop_csv(table_name)
        print(f"Table {table_name} dropped")
    
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
    
    def get_info_database(self) -> dict:
        '''
        Returns the metadata of the database.

        Returns:
            dict: The metadata of the database.
        '''
        return self.metadata

    def get_tables(self) -> list:
        '''
        Returns the list of tables in the database.

        Returns:
            list: The list of tables in the database.
        '''
        return list(self.metadata.keys())    

    def drop_database(self) -> None:
        '''
        Drops the database.
        '''
        try:
            for table in self.metadata.keys():
                self.drop_table(table)
        except:
            pass
        self.metadata = {}
        self.tables = {}
        os.remove(self.file_path + "metadata")
        os.rmdir(self.file_path)

        raise DroppedDatabaseError()
