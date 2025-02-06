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

    def create_database_folder(self) -> None:
        """
        Creates the database folder and metadata file if they do not exist.
        """
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)
        
        if not os.path.exists(self.file_path + "metadata"):
            with open(self.file_path + "metadata", "wb") as file:
                pickle.dump({}, file)

    def save_table(self, table_name:str, table: list) -> None:
        """
        Saves the table to the database.

        Parameters:
            table_name (str): The name of the table.
            table (list): The table data.
        """
        with open(self.file_path + table_name + ".csv", "w") as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerows(table)
            file.close()    

    def create_csv(self, table_name: str, metadata_table:dict) -> None:
        """
        Creates a new table in the database.

        Parameters:
            table_name (str): The name of the table.
            columns (list): The list of column names.
            data_types (list): The list of data types for the columns.
            primary_key (str): The primary key column.
            foreign_keys (dict): The foreign keys for the table.
        """
        
        
        if table_name in self.load_metadata().keys():
            print("Table already exists")
            return

        self.add_to_metadata(metadata_table)
        with open(self.file_path + table_name + ".csv", "w") as file:
            file.close()

    def load_csv(self, table_name:str) -> list:
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
            raise FileNotFoundError("Table not found")

    def insert_row_csv(self, table_name:str, row: list) -> None:
        """
        Inserts a row to the table to the database.

        Parameters:
            table_name (str): The name of the table.
            row (list): The row will be added.
        """
        table = self.load_csv(table_name)
        table.append(row)
        self.save_table(table_name, table)

    def update_rows(self, table_name:str,   metadata_table , *conditions , **kwargs) -> None:  
        
        """
        Updates a row to the table.

        Parameters:
            table_name (str): The name of the table.
            conditions (list): The conditions in functions to update the row.
            kwargs (dict): The columns and values to update.
        """
        table = self.load_csv(table_name)
        
        for row in table:
            if all(condition(row, metadata_table) for condition in conditions):
                for key, value in kwargs.items():
                    row[metadata_table["columns"].index(key)] = value
        self.save_table(table_name, table)

    def delete_rows(self, table_name:str, metadata_table, *conditions) -> None:
        """
        Deletes a rows to the table.

        Parameters:
            table_name (str): The name of the table.
            conditions (list): The conditions in functions to delete the row.
        """
        table = self.load_csv(table_name)
        
        
        table = [row for row in table if all(condition(row, metadata_table) for condition in conditions)]
        self.save_table(table_name, table)


if __name__ == "__main__":
    db = DatabaseFileManager("test")
    print(db.metadata)
    print(db.load_table("test"))
    db.create_table("test_table", columns=["id", "name"], data_types=["int", "str"], primary_key="id", foreign_keys=None)
    print(db.metadata)
    
        