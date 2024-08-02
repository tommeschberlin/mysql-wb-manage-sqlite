3: Views
  * what about views of changed tables definition
  * tests for it

6: 2024-07-31 '/' und '.' in name was not found by check

7: 2024-08-01 Overwrite Message -> Update Message

8: 2024-08-01 Datentyp Date -> Datetime, Bit -> INT

9: 2024-08-01 Wieso erlaubt Numeric Strings ?
  
________________________________________________________________________________

# Solved

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


 
