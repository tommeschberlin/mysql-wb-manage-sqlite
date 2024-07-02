4: Backup/restore of data without table column names at every row should be possible

3: Views
  * what about views of changed tables definition
  * tests for it


________________________________________________________________________________

# Solved

2: Update Readme
  * Table/Columns now changeable

1: Add check for colum- and tablenames
  * no Spaces
  * no Umlauts/special characters
  * mostly only this regexp should match: "^[a-zA-Z+-_]*$"
  * add test for it


 
