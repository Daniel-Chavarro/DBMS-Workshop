#import shlex
from database import Database
from parser import Parser


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
                

            else:
                parse = Parser(command_str)
                print(
                parse.parse()
                )

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()