# from file_manager import DatabaseFileManager

# class Database:
#     def __init__(self, db_name:str):
#         self.db_name = db_name
#         self.file_manager = DatabaseFileManager(db_name)
#         self.metadata = self.file_manager.load_metadata(self.file_manager.file_path + "metadata")
    
#     def insert_row(self, table_name:str, row: list) -> None:
#         """
#         Inserts a row to the table to the database.

#         Parameters:
#             table_name (str): The name of the table.
#             row (list): The row will be added.
#         """
#         table = self.load_table(table_name)
#         table.append(row)
#         self.file_manager.(table_name, table)

#     def load_metadata(self, filename: str) -> dict:
#         """
#         Loads the metadata from the specified file.

#         Parameters:
#             filename (str): The path to the metadata file.

#         Returns:
#             dict: The metadata of the database.
#         """
#         try:
#             with open(filename, "rb") as file:
#                 return pickle.load(file)
#         except FileNotFoundError:
#             return {}
        
#     def add_to_metadata(self ,metadata:dict) -> None:
#         """
#         Adds the metadata to the metadata file.

#         Parameters:
#             metadata (dict): The metadata to save.
#         """
#         filename = self.file_path + "metadata"
#         with open(filename, "wb") as file:
#             pickle.dump(metadata, file)

        