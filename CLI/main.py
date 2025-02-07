# This file is the main file for the CLI. It takes the input from the user and calls the parser to parse the input and execute the command.
# The main function runs a loop that takes the input from the user and executes the command until the user types 'exit'.
# The main function also displays the help message when the user types 'help'.
# The main function catches any exceptions raised during the execution of the command and displays an error message.

import sys
import os
# Add project root (UDSQL) to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now import dbms modules
from dbms.database import Database
from dbms.parser import Parser

def main():
    db_name = input("Enter the database name:")
    db = Database(db_name)
    print("\nDatabase CLI - Type 'help' to see all commands.")

    while True:
        command = input("db>")
        command_str = command

        if command.lower() == "exit":
            print("Exiting CLI.bye-bye")
            break

        try:

            if command == "help":
                
                print(
                """
                create table <table_name> <column_name1> <data_type1> <column_name2> <data_type2> ... <primary_key> <foreign_key1> <foreign_key2> ...
                insert into <table_name> values <value1> <value2> <value3> ...
                select * from <table_name>
                update <table_name> set <column_name1> <value1> <column_name2> <value2> ... where <condition>
                delete from <table_name> where <condition>
                exit
                """
                )

            else:
                parse = Parser(command_str)
                print(
                parse.parse()
                )

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()