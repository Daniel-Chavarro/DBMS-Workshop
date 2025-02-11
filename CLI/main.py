# This file is the main file for the CLI. It takes the input from the user and calls the parser to parse the input and execute the command.
# The main function runs a loop that takes the input from the user and executes the command until the user types 'exit'.
# The main function also displays the help message when the user types 'help'.
# The main function catches any exceptions raised during the execution of the command and displays an error message.

import sys
import os
import time
# Add project root (UDSQL) to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now import dbms modules
from dbms.database import Database
from dbms.parser import Parser
from dbms.executor import Executor
from dbms.exeptions import DroppedDatabaseError

def main():
    while True:
        
        if not os.path.exists("data"):
            os.makedirs("data")
        clear_screen()
        print("Welcome to UDSQL")
        print(f"""Available databases:
              {', '.join(get_databases_aviable())}""")
        print("If want to create a new database, type the name of the database.")
        print("If you want to exit, type 'exit'")
         
        db_name = input("Enter the database name:")
        if db_name == "exit":
            print("Exiting CLI.bye-bye")
            return
        clear_screen()
        db = Database(db_name)
        print("\nDatabase CLI - Type 'help' to see all commands.")

        while True:
            command = input("db>")
            command_str = command

            if command.lower() == "exit":
                print("Exiting CLI.bye-bye")
                return

            try:

                if command == "help":
                    
                    print(
                    """
                    CREATE TABLE <table_name> <column_name1> <data_type1> <column_name2> <data_type2> ... PRIMARY_KEY <primary_key> FOREIGN_KEY(OPTIONAL) <foreign_key1>
                    INSERT INTO <table_name> VALUES <value1> <value2> <value3> ...
                    SELECT * FROM <table_name> WHERE <condition>
                    UPDATE <table_name> SET <column_name1> <value1> <column_name2> <value2> ... WHERE <condition>
                    DELETE FROM <table_name> WHERE <condition>
                    DROP TABLE <table_name>
                    DROP DATABASE 
                    EXIT
                    """
                    )

                else:
                    parse = Parser(command_str)
                    executor = Executor(db)
                    executor.execute(*parse.parse())
            except DroppedDatabaseError as e:
                print(f"{e}")
                time.sleep(2)
                break
            except Exception as e:
                print(f"Error: {e}")

def get_databases_aviable():
    return os.listdir("data")

def clear_screen():
    os.system("cls")

if __name__ == "__main__":
    main()