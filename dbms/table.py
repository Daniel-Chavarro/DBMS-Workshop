from tabulate import tabulate


class Table:
    def __init__(self, name, database):
        """Initialize a table inside a database."""
        self.name = name
        self.database = database  # Parent database
        self.metadata = database.metadata[name]
        self.file_manager = database.file_manager

    def insert_row(self, row):
        """Inserts a row into the table, ensuring it follows schema."""
        self.apply_data_type_input_list(row)
        self.file_manager.insert_row_csv(self.name, row)
        print(f"Succesfully inserted row into {self.name}")

    def update(self, condition_fn, condition_str , update_values:dict):
        """Updates rows based on a condition."""
        self.apply_data_type_input_dict(update_values)
        self.file_manager.update_rows(self.name, self.metadata , condition_fn, condition_str , update_values)
        print(f"Succesfully updated row into {self.name}")

    def delete(self, condition_fn, condition_str):
        """Deletes rows matching a condition."""
        self.file_manager.delete_rows(self.name, self.metadata , condition_fn, condition_str)
        print(f"Succesfully deleted row into {self.name}")
    
    
    def select(self, columns, condition_fn=None, condition_str=None):
        """
        Selects rows based on columns and conditions.

        Parameters:
            columns (list): The list of columns to select. Use ['*'] to select all columns.
            condition_fn (function): The function to evaluate conditions on rows.
            condition_str (str): The condition string to pass to the condition function.

        Returns:
            list: The selected rows.
        """
        table = self.file_manager.load_csv(self.name)
        selected_rows = []

        for row in table:
            if any(row):  # Check if the row is not empty
                if condition_fn is None or condition_fn(row, self.metadata, condition_str):
                    if columns == ['*']:
                        selected_rows.append(row)
                    else:
                        selected_row = [row[self.metadata['columns'].index(col)] for col in columns]
                        selected_rows.append(selected_row)

        self.print_selected_rows(selected_rows, columns)

    def print_selected_rows(self, selected_rows, columns):
        """
        Prints the selected rows in a tabular format.

        Parameters:
            selected_rows (list): The list of selected rows.
            columns (list): The list of columns to select. Use ['*'] to select all columns.
        """
        if columns == ['*']:
            columns = self.metadata['columns']
        
        # Print the table using tabulate
        print(tabulate(selected_rows, headers=columns, tablefmt="grid"))

    
    def apply_data_type_input_dict(self, values:dict):
        """
        Applies the data types to the input values.

        Parameters:
            values (dict): The values to apply the data types.
        """
        data_types = self.metadata["data_types"]
        columns = self.metadata["columns"]
        
        for key, value in values.items():
            if key not in columns:
                raise ValueError("Bad input values")
            i = columns.index(key)
            if data_types[i] == "int":
                try:
                    values[key] = int(value)
                except:
                    raise ValueError("Bad input values")
            elif data_types[i] == "float":
                try:
                    values[key] = float(value)
                except:
                    raise ValueError("Bad input values")
            elif data_types[i] == "str":
                values[key] = str(value)
            else:
                raise ValueError("Bad input values")
        

    def apply_data_type_input_list(self, values:list) -> None:
        '''
        Applies the data types to the input values.

        Parameters:
            table_name (str): The name of the table.
            values (list/dict): The values to apply the data types.
        '''
        
        data_types = self.metadata["data_types"]
        if len(values) != len(data_types):
            raise ValueError("Bad input values")
        
        for i in range(len(values)):
                if data_types[i] == "int":
                    try:
                        values[i] = int(values[i])
                    except:
                        raise ValueError("Bad input values")
                elif data_types[i] == "float":
                    try:
                        values[i] = float(values[i])
                    except:
                        raise ValueError("Bad input values")
                elif data_types[i] == "str":
                    values[i] = str(values[i])
                else:
                    raise ValueError("Bad input values")
        