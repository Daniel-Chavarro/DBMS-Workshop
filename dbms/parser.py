
class Parser:
    def __init__(self, msg):
        self.msg = msg
        self_lex = None
        self.command = None
        self.conditions = None
        self.table = None
        self.args = {}

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
                self.table = self.lex[2]
                columns = []
                data_types = []
                i = 3
                try:
                    while self.lex[i].upper() != 'PRIMARY_KEY':
                        column, data_type = self.lex[i:i+2]
                        columns.append(column)
                        data_types.append(data_type)
                        i += 2
                    
                except:
                    raise ValueError("Invalid command")
                self.args["columns"] = columns
                self.args["data_types"] = data_types
                self.args["primary_key"] = self.lex[i+1]
                try:
                    self.args.append({"foreign_keys": self.lex[i+2:]})
                except:
                    pass
                return self.command, self.table, self.args
            
            # Database operation
            elif self.lex[1].upper() == "DATABASE":
                self.command = 1
                self.args = self.lex[2]
                return self.command, self.args

            else:
                raise ValueError("Invalid command")

        # INSERT operation
        elif self.lex[0].upper() == "INSERT" and self.lex[1].upper() == "INTO":
            self.command = 3
            self.table = self.lex[2]
            self.args = self.lex[4:]
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
            flag = True
            for i in range(2, len(self.lex)):
                if self.lex[i].upper() == "WHERE":
                    flag = False
                    argument = self.lex[3:i]
                    self.args = self.normalize(argument, 2)
                    condition = self.lex[i+1:]
                    self.conditions = " ".join(condition).split(",")
                    break
            if flag:
                argument = self.lex[3:]
                self.args = self.normalize(argument, 2)
            return self.command, self.table, self.args, self.conditions
        
        # DELETE operation
        elif self.lex[0].upper() == "DELETE" and self.lex[1].upper() == "FROM":
            self.command = 6
            self.table = self.lex[2]
            for i in range(len(self.lex)):
                if self.lex[i].upper() == "WHERE":
                    condition = self.lex[i+1:]
                    self.conditions = " ".join(condition).split(",")
                    break
            return self.command, self.table, self.conditions
        # DROP operation
        elif self.lex[0].upper() == "DROP":
            if self.lex[1].upper() == "DATABASE":
                self.command = 8
                return [self.command]
            elif self.lex[1].upper() == "TABLE":
                self.command = 7
                self.table = self.lex[2]
                return self.command, self.table
            else:
                raise ValueError("Invalid command")
        else:
            raise ValueError("Invalid command")

    def normalize(self, value:list[str], n_to_group:int) -> list[str]:
        """
        Normalizes the values of the list, deleting spaces and divide it in places.

        Parameters:
            value (list): The list of values to normalize.

        Returns:
            list: A list of normalized values.
        """
        output = {}
        partition = value
        while len(partition) > 0:
            output[partition[0]] = partition[1]
            partition = partition[2:]
        
        return output

         
         

"""
if __name__ == "__main__":
    parser = Parser("CREATE TABLE students name VARCHAR20 age int grade int PRIMARY_KEY name")
    print(parser.parse())
    parser = Parser("INSERT INTO students John 20 3.5")
    print(parser.parse())
    parser = Parser("SELECT name age FROM students WHERE age > 20")
    print(parser.parse())
    parser = Parser("UPDATE students SET age 21 grade 4.0 WHERE name == John")
    print(parser.parse())
    parser = Parser("DELETE FROM students WHERE age > 20")
    print(parser.parse())
    parser = Parser("UPDATE students SET age 21  WHERE name == John")
    print(parser.parse())
"""