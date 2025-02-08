from dbms.database import Database

class Executor:
    def __init__(self, db):
        # self.db = Database(db)
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
            case _:
                raise ValueError("Invalid command")
    
    def condition_fn(row, metadata, condition_str):
        """Safely evaluate a condition string by mapping column names to values."""
        col_names = metadata["columns"]  # Example: ["id", "name", "age"]
        col_values = {col: row[i] for i, col in enumerate(col_names)}
        return eval(condition_str, {"__builtins__": None}, col_values)   

    def create_table(self, table_name: str, columns: list, data_types: list, primary_key: str, foreign_keys: list = []):
        self.db.create_table(table_name, columns, data_types, primary_key, foreign_keys)
    
    def insert_into(self, table_name: str, values: list):
        self.db.tables[table_name].insert_row(values)
    
    def update(self, table_name: str, condition_str: str, update_values:dict):
        self.db.tables[table_name].update(self.condition_fn, condition_str, update_values)

    def delete(self, table_name: str, condition_str: str):
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
        table = self.db.tables[table_name]
        if condition_str:
            print(table.select(columns, self.condition_fn, condition_str))
        else:
            print(table.select(columns))