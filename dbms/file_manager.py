"""
file_manager.py

This module provides the DatabaseFileManager class for managing database files and metadata.
"""

import csv
import os
import pickle



class DatabaseFileManager:
    """
    A class to manage database files and metadata.

    Attributes:
        database_name (str): The name of the database.
        file_path (str): The path to the database files.
        metadata (dict): The metadata of the database.
    """

    def __init__(self, database_name:str):
        """
        The constructor for DatabaseFileManager class.

        Parameters:
            database_name (str): The name of the database.
        """

        ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.database_name = database_name
        self.file_path: str = os.path.join(ROOT_DIR,'data' ,database_name, '')
        self.create_database_folder()
        self.metadata = self.load_metadata((self.file_path + "metadata"))  

    def create_database_folder(self) -> None:
        """
        Creates the database folder and metadata file if they do not exist.
        """
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)
        
        if not os.path.exists(self.file_path + "metadata"):
            with open(self.file_path + "metadata", "wb") as file:
                pickle.dump({}, file)

        
        
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
            return {}
        
    def add_to_metadata(self ,metadata:dict) -> None:
        """
        Adds the metadata to the metadata file.

        Parameters:
            metadata (dict): The metadata to save.
        """
        filename = self.file_path + "metadata"
        with open(filename, "wb") as file:
            pickle.dump(metadata, file)

    def create_table(self, table_name: str, columns: list, data_types: list, primary_key: list, foreign_keys: list) -> None:
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
            print("Table already exists")
            return

        self.metadata[table_name] = {
            "columns": columns,
            "data_types": data_types,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys
            }
    
        self.add_to_metadata(self.metadata)
        with open(self.file_path + table_name + ".csv", "w") as file:
            file.close()

    def load_table(self, table_name:str) -> list:
        """
        Loads a table from the database.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            list: The table data.
        """
        
        try:
            with open(self.file_path + table_name + ".csv", "r") as file:
                reader = csv.reader(file, delimiter='|')
                table = [row for row in reader]
                file.close()
                return table
        except FileNotFoundError:
            return "Table not found"

    def save_table(self, table_name:str, table: list) -> None:
        """
        Saves a table to the database.

        Parameters:
            table_name (str): The name of the table.
            table (list): The table data to save.
        """

        with open(self.file_path + table_name + ".csv", "w") as file:
            writer = csv.writer(file, delimiter='|')
            for row in table:
                writer.writerow(row)
            file.close()

    def insert_row(self, table_name:str, row: list) -> None:
        """
        Inserts a row to the table to the database.

        Parameters:
            table_name (str): The name of the table.
            row (list): The row will be added.
        """
        table = self.load_table(table_name)
        table.append(row)
        self.save_table(table_name, table)

    def update_row(self, table_name:str, *conditions , **kwargs) -> None:  
        
        """
        Updates a row to the table.

        Parameters:
            table_name (str): The name of the table.
            conditions (list): The conditions in functions to update the row.
            kwargs (dict): The columns and values to update.
        """
        table = self.load_table(table_name)
        
        try:
            metadata_table = self.metadata[table_name]
        except KeyError:
            assert False, "Table not found"
        
        for row in table:
            if all(condition(row, metadata_table) for condition in conditions):
                for key, value in kwargs.items():
                    row[metadata_table["columns"].index(key)] = value
        self.save_table(table_name, table)

    def delete_row(self, table_name:str, *conditions) -> None:
        """
        Deletes a row to the table.

        Parameters:
            table_name (str): The name of the table.
            conditions (list): The conditions in functions to delete the row.
        """
        table = self.load_table(table_name)
        try:
            metadata_table = self.metadata[table_name]
        except KeyError:
            assert False, "Table not found"
        
        table = [row for row in table if any(condition(row, metadata_table) for condition in conditions)]
        self.save_table(table_name, table)


if __name__ == "__main__":
    db = DatabaseFileManager("test")
    print(db.metadata)
    print(db.load_table("test"))
    db.create_table("test_table", columns=["id", "name"], data_types=["int", "str"], primary_key="id", foreign_keys=None)
    print(db.metadata)
    
        