3: Views
  * what about views of changed tables definition
  * tests for it

7: 2024-08-01 Overwrite Message -> Update Message

8: 2024-08-01 Datentyp Date -> Datetime, Bit -> INT, eventually use STRICT keyword,
   see: https://www.sqlite.org/stricttables.html

10: 2024-08-19 If some warnings in log, show a rollback button or some dialog to track attention
  
________________________________________________________________________________

# Solved

9: 2024-09-04 Why can have sqlite strings in numeric declared columns ?
   SQLite has a dynamic typing, see: https://www.sqlite.org/datatype3.html

6: 2024-07-31 '/' und '.' in name was not found by check

5: 2024-08-02 Keywords in table column names
  * Problems with "Alter" as col name, was recognized as sql keyword, at restore of table data with rowCol strategy

4: 2024-07-02 Accelaration for backup/restore of data without table column names at every row should be possible
   @see https://stackoverflow.com/questions/1609637/how-to-insert-multiple-rows-in-sqlite

2: Update Readme
  * Table/Columns now changeable

1: Add check for colum- and tablenames
  * no Spaces
  * no Umlauts/special characters
  * mostly only this regexp should match: "^[a-zA-Z+-_]*$"
  * add test for it


 
