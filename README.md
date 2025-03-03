# UDSQL CLI - Simple Database Management System

UDSQL is a simple command-line interface (CLI) for managing databases. It allows you to create, insert, select, update, and delete tables and records in a local database system. This project is designed to demonstrate basic database operations using Python.

## Features

- **Create a Database**: Create a new database.
- **Create Tables**: Define tables with specified columns and data types.
- **Insert Data**: Add records into tables.
- **Select Data**: Retrieve records from tables.
- **Update Data**: Modify records in tables.
- **Delete Data**: Remove records from tables.
- **Drop Tables/Databases**: Remove tables or entire databases.
- **CLI Help**: Type `help` to view the available commands and their syntax.

## Project Structure

```
UDSQL/
│-- dbms/                 # Core database management system
│   │-- database.py       # Handles database creation and operations
│   │-- executor.py       # Executes SQL-like commands
│   │-- exceptions.py     # Custom error handling
│   │-- file_manager.py   # Manages file storage of databases
│
│-- data/                 # Storage location for created databases
│
│-- cli/                  # Command-line interface
│   │-- main.py           # Entry point for interacting with UDSQL
│
│-- README.md             # Documentation
│-- requirements.txt      # Dependencies list
```

## Requirements

- Python 3.x
- The following Python modules:
  - `os`
  - `sys`
  - `time`
  - `tabulate 0.9.0`

### Install Dependencies

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

## Setup

### Clone the Repository

First, clone the repository to your local machine using Git:

```bash
git clone https://github.com/Daniel-Chavarro/DBMS-Workshop.git
```

Navigate to the project directory:

```bash
cd DBMS-Workshop
```

## Usage

Navigate to the `cli` directory and run the main script:

```bash
python CLI/main.py
```

### Basic Commands

- **Create a table:**
  ```sql
  CREATE TABLE users id int name str age int
  ```
- **Insert data:**
  ```sql
  INSERT INTO users VALUES 1 Alice 25
  ```
- **Select data:**
  ```sql
  SELECT * FROM users
  ```
- **Update data:**
  ```sql
  UPDATE users SET age = 26 WHERE name == 'Alice'
  ```
- **Delete data:**
  ```sql
  DELETE FROM users WHERE name == 'Alice'
  ```
- **Drop a table:**
  ```sql
  DROP TABLE users
  ```
- **Drop a database:**
  ```sql
  DROP DATABASE
  ```

Full commands in documentation.
