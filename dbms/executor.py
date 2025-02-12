from dbms.database import Database

class Executor:
    """
    Executes the commands on the database.
    
    Attributes:
        db (Database): The database object.
        metadata (dict): The metadata of the database.
        file_path (str): The path to the database file.
    """
    def __init__(self, db):
        self.db = db
        self.metadata = db.metadata
        self.file_path = db.file_path
    
    def execute(self, command, *args):
        """
        Executes the command with the given arguments.

        Parameters:
            command (int): The command to execute.
            args (tuple): The arguments for the command.
        """


        match command:
            case 2:
                self.create_table(
                    args[0], args[1]["columns"], args[1]["data_types"], args[1]["primary_key"], args[1].get("foreign_keys", []))
            case 3:
                self.insert_into(*args)
            case 4:
                self.select(*args)
            case 5:
                self.update(*args)
            case 6:
                self.delete(*args)
            case 7:
                self.drop_table(args[0])
            case 8:
                self.drop_database()
            case _:
                raise ValueError("Invalid command")
    
    @staticmethod
    def condition_fn(row, metadata, condition_str):
        """Safely evaluate a condition string by mapping column names to values."""
        col_names = metadata["columns"]  # Example: ["id", "name", "age"]
        col_values = {col: row[i] for i, col in enumerate(col_names)}
        return eval(condition_str, {"__builtins__": None}, col_values)   

    def create_table(self, table_name: str, columns: list, data_types: list, primary_key: str, foreign_keys=None):
        '''Creates a table in the database.
        
        Parameters:
            table_name (str): The name of the table.
            columns (list): The list of columns.
            data_types (list): The list of data types for the columns.
            primary_key (str): The primary key column.
            foreign_keys (list): The list of foreign keys.
        
        '''
        if foreign_keys is None:
            foreign_keys = []
        self.db.create_table(table_name, columns, data_types, primary_key, foreign_keys)
    
    def insert_into(self, table_name: str, values: list):
        '''Inserts a row into the table.
        
        Parameters:
            table_name (str): The name of the table.
            values (list): The list of values to insert.
        '''
        if table_name not in self.db.tables:
            raise ValueError("Table not found")
        self.db.tables[table_name].insert_row(values)
    
    def update(self, table_name: str, update_values:dict, condition_str: str ):
        '''Updates rows in the table based on the condition.
        
        Parameters:
            table_name (str): The name of the table.
            update_values (dict): The values to update.
            condition_str (str): The condition string.
        '''
        if table_name not in self.db.tables:
            raise ValueError("Table not found")
        if condition_str != None:
            condition_str = "".join(condition_str)
        else:
            condition_str = "True"
        self.db.tables[table_name].update(self.condition_fn, condition_str, update_values)

    def delete(self, table_name: str, condition_str: list):
        '''Deletes rows from the table based on the condition.
        
        Parameters:
            table_name (str): The name of the table.
            condition_str (list): The condition string.
        '''
        
        if table_name not in self.db.tables:
            raise ValueError("Table not found")
        if condition_str != None:
            condition_str = "".join(condition_str)
        else:
            condition_str = "True"
        self.db.tables[table_name].delete(self.condition_fn, condition_str)

    def select(self, table_name: str, columns: list, condition_str: str = None):

        """
        Selects rows from a table based on columns and conditions.

        Parameters:
            table_name (str): The name of the table.
            columns (list): The list of columns to select. Use ['*'] to select all columns.
            condition_str (str): The condition string to pass to the condition function.

        Returns:
            list: The selected rows.
        """
        if table_name not in self.db.tables:
            raise ValueError("Table not found")
        table = self.db.tables[table_name]
        if condition_str != None:
            condition_str = "".join(condition_str)  
            table.select(columns, self.condition_fn, condition_str)
        else:
            table.select(columns)
    
    def drop_table(self, table_name: str):
        '''Drops a table from the database.
        
        Parameters:
            table_name (str): The name of the table.
            '''
        self.db.drop_table(table_name)
    
    def drop_database(self):
        '''Drops the database.'''
        self.db.drop_database()