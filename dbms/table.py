class Table:
    def __init__(self, name, database):
        """Initialize a table inside a database."""
        self.name = name
        self.database = database  # Parent database
        self.metadata = database.metadata[name]
        self.file_manager = database.file_manager

    def insert_row(self, row):
        """Inserts a row into the table, ensuring it follows schema."""
        self.file_manager.insert_row_csv(self.name, row)

    def update(self, condition_fn, condition_str , update_values:dict):
        """Updates rows based on a condition."""
        self.file_manager.update_rows(self.name, self.metadata , condition_fn, condition_str , update_values)

    def delete(self, condition_fn, condition_str):
        """Deletes rows matching a condition."""
        self.file_manager.delete_rows(self.name, self.metadata , condition_fn, condition_str)
    
    
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

        return selected_rows


    # def select(self, columns, condition_fn, condition_str):
    #     """Selects rows based on columns and conditions."""
        
    #     table = self.file_manager.load_csv(self.name)
    #     for row in table:
    #         if condition_fn(row, self.metadata, condition_str):
    #             print(row)