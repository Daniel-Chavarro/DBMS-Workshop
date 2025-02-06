class Table:
    def __init__(self, name, database):
        """Initialize a table inside a database."""
        self.name = name
        self.database = database  # Parent database
        self.metadata = database.metadata[name]
        self.file_manager = database.file_manager

    def insert(self, row):
        """Inserts a row into the table, ensuring it follows schema."""
        self.file_manager.insert_row(self.name, row)

    def update(self, *condition_fn, **update_values):
        """Updates rows based on a condition."""
        self.file_manager.update_rows(self.name, condition_fn, update_values)

    def delete(self, *conditions_fn):
        """Deletes rows matching a condition."""
        self.file_manager.delete_rows(self.name, self.metadata , conditions_fn)