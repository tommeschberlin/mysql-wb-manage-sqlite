# MySQL Workbench ManageSQLite Extension

ManageSQLite is an extension for
[MySQL Workbench](http://www.mysql.com/products/workbench/) to export a schema
catalog as SQLite's CREATE SQL queries and/or create/update SQLite databases.
Based on [MySQL Workbench ExportSQLite Plugin](https://github.com/ssstain/mysql-wb-exportsqlite) for using with
MySQL Workbench 8.0.40 or above.

## Installation

a) in your userprofile

1. Open MySQL Workbench
2. Choose "Install Plugin/Module..." in "Scripting" menu
3. Open **`manage_sqlite_grt.py`** file
4. Copy **`SQLiteDbUpdater.py`** to "User Module File Location" too, see [here](https://dev.mysql.com/doc/workbench/en/wb-modules.html)
5. Restart MySQL Workbench

b) gobal

1. Copy the files **`manage_sqlite_grt.py`** and **`SQLiteDbUpdater.py`** into the MySQL Workbench directory (usually in **`c:\Programs`**) into its **`modules`** directory.
5. Start MySQL Workbench

## Usage

Once you open a database model in MySQL Workbench, you can see "Manage SQLite database" in "Tools > Catalog" menu. Just choose it.


## Some comments to data preserving

The **SQLiteDbUpdater** tries to keep your existing table data, but SQLite can not alter tables with foreign keys. So currently it creates the new db with a temporarily choosen name, creates a sql-dump from the old db, restores it into the new created db. After successful restoring **and only then**, the old db will be deleted and the new db will be renamed. Restoring follows this strategies:
1) **Copy complete rows**  
   Works fine for all unchanged tables of your database
2) **Copy rows column by column matching their names**  
   Matches the column names of changed tables and puts the data in. Thus you can alter table defintions by adding, moving or removing table columns.
3) **Copy complete rows even when columnames has changed**  
   Check if column names have changed, but not their order at all. Data of columns with changed column name will be kept.
4) **Indexing**
   Indicees in existing database will not be considered, because all indicees should come from the workbench model
5) **Views**
   Views in existing databas will be restored, renamed tablenames and columnnames will be detected and adapted (hopefully)

## Restrictions / Problems

1) the SQLite ODBC driver causes problems at import to ms-access if tables contain indicees for foreign keys, 
   because of MYSQL Workbench will put into a '.' the names. This '.' will be automatically replaced by a '_'
2) Creation of SQL code works not for **`TIMESTAMP UPDATE`**, therefore do not use auto updating timstamps for now
3) If changing of a column type leads to a more restricive type, you should alter your data before changing the type
4) Changing the name of tables for existing db's is not supported for now. You have to backup/restore tabledata by yourself

## License

The original Lua plugin is released under GPLv3 so this Python version
inherits it.
