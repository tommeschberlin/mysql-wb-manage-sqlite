# MySQL Workbench ManageSQLite Extension

ManageSQLite is an extension for
[MySQL Workbench](http://www.mysql.com/products/workbench/) to export a schema
catalog as SQLite's CREATE SQL queries and/or create/update a SQLite databases.
Based on [MySQL Workbench ExportSQLite Plugin](https://github.com/ssstain/mysql-wb-exportsqlite) for using with
MySQL Workbench 8.0 or above.

## Installation

a) in your userprofile

1. Open MySQL Workbench
2. Choose "Install Plugin/Module..." in "Scripting" menu
3. Open `manage_sqlite_grt.py` file
4. Restart MySQL Workbench

b) gobal

Copy the ```manage_sqlite_grt.py``` file into the MySQL Workbench directory (usually in ```c:\Programs```) into its ```modules``` directory.

## Usage

Once you open a database model in MySQL Workbench, you can see "Manage SQLite database" in "Tools > Catalog" menu. Just choose it.

## License

The original Lua plugin is released under GPLv3 so this Python version
inherits it.
