class DroppedDatabaseError(Exception):
    """ Raised when a database has been dropped 
    
    Attributes:
        message (str): The message of the error.
    """
    def __init__(self, message="Database has been dropped"):
        self.message = message
        super().__init__(self.message)