
class Parser:
    def __init__(self, msg):
        self.msg = msg
        self_lex = None
        self.command = None
        self.conditions = None
        self.table = None
        self.args = None

    def parse(self):
        """
        Parses the message and returns the command and the arguments.

        Returns:
            tuple: A tuple containing the command and the arguments.
        """
        self.lex = self.msg.split()
        
        # CREATE operation
        if self.lex[0].upper() == "CREATE":
            # Table operation
            if self.lex[1].upper() == "TABLE":
                self.command = 2
                self.args = self.lex[2:]
                return self.command, self.args
            
            # Database operation
            elif self.lex[1].upper() == "DATABASE":
                self.command = 1
                self.args = self.lex[2]
                return self.command, self.args
        
        # INSERT operation
        elif self.lex[0].upper() == "INSERT" and self.lex[1].upper() == "INTO":
            self.command = 3
            self.table = self.lex[2]
            self.args = self.lex[3:]
            return self.command, self.table, self.args
        
        # SELECT operation
        elif self.lex[0].upper() == "SELECT":
            self.command = 4
            for i in range(1, len(self.lex)):
                if self.lex[i].upper() == "FROM":
                    self.table = self.lex[i+1]
                    self.args = self.lex[1:i]
                    break
            if "WHERE" in self.lex:
                for i in range(len(self.lex)):
                    if self.lex[i].upper() == "WHERE":
                        condition = self.lex[i+1:]
                        self.conditions = " ".join(condition).split(",")
                        break
            return self.command, self.table, self.args, self.conditions
        
        # UPDATE operation
        elif self.lex[0].upper() == "UPDATE" and self.lex[2].upper() == "SET":
            self.command = 5
            self.table = self.lex[1]
            for i in range(2, len(self.lex)):
                if self.lex[i].upper() == "WHERE":
                    argument = self.lex[3:i]
                    self.args = " ".join(argument).split(",")
                    condition = self.lex[i+1:]
                    self.conditions = " ".join(condition).split(",")
                    break
            return self.command, self.table, self.args, self.conditions
        
        # DELETE operation
        elif self.lex[0].upper() == "DELETE" and self.lex[1].upper() == "FROM":
            self.command = 6
            self.table = self.lex[2]
            if "WHERE" in self.lex:
                for i in range(len(self.lex)):
                    if self.lex[i].upper() == "WHERE":
                        condition = self.lex[i+1:]
                        self.conditions = " ".join(condition).split(",")
                        break
            return self.command, self.table, self.conditions
        else:
            raise ValueError("Invalid command")
            
