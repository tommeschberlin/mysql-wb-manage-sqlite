import sys
import os
import re
import unittest
import sqlite3

# Get the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))# Get the parent directory by going one level up
parent_dir = os.path.dirname(current_dir)# Add the parent directory to sys.path
sys.path.append(parent_dir)

import SQLiteDbUpdater

class TestSQLiteUpdater(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.workDir = os.path.join( 'c:/', 'tmp' )
        if not os.path.exists( self.workDir ):
            raise Exception( 'Error', 'No workDir \"%s\" found!' % self.workDir )
        self.dbOrigName = "test"
        self.dbOrigFileName = self.dbOrigName + ".sqlite"
        self.dbOrigPath = os.path.join( self.workDir, self.dbOrigFileName )
        self.tableColsSQL = {
            'kurs': [
                '"id_kurs" INTEGER PRIMARY KEY NOT NULL',
                '"kursname" VARCHAR(45)' ],
            'teilnehmer': [
                '"id_teilnehmer" INTEGER PRIMARY KEY NOT NULL',
                '"name" VARCHAR(45)',
                '"kursid" INTEGER REFERENCES kurs (id_kurs)' # foreign key !!
            ]
        }
    
    def setUp(self):
        if os.path.isfile(self.dbOrigPath):
            os.remove( self.dbOrigPath )

        sql = self.getDbCreationSQL(self.tableColsSQL)
        self.executeSqlScript(self.dbOrigFileName, sql)

    def getDbCreationSQL(self, tableColsSQL ):
        sql  = 'ATTACH "%s" AS "test";\n' % self.dbOrigFileName
        sql += 'BEGIN;\n'

        sql += 'CREATE TABLE "test"."kurs"(\n'
        sql += ',\n'.join( tableColsSQL['kurs'] )
        sql += ');\n'

        sql += 'CREATE TABLE "test"."teilnehmer"(\n'
        sql += ',\n'.join( tableColsSQL['teilnehmer'] )
        sql += ');\n'

        sql += 'CREATE INDEX "test"."teilnehmer.kursId_idx" ON "teilnehmer" ("kursid");\n'
        sql += 'COMMIT;\n'

        return sql

    def executeSqlScript(self, dbFileName, sql):
        os.chdir( self.workDir )
        conn = sqlite3.connect(dbFileName)
        try:        
            cur = conn.cursor()
            cur.executescript(sql)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def executeSqlLine(self, dbFileName, sql):
        os.chdir( self.workDir )
        conn = sqlite3.connect(dbFileName)
        try:        
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            result = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        return result

    def getTableRows(self, dbFileName, tableName ):
        rows = self.executeSqlLine(dbFileName, "select * from \"%s\"" % tableName )
        return rows

    @unittest.skip("skipped temporarily")
    def test_RestoreByRowStrategy(self):
        # add data
        sql  = 'INSERT INTO "kurs" VALUES(1, "Hüpfen");'
        sql += 'INSERT INTO "teilnehmer" VALUES(1, "Shwze", 1);'
        self.executeSqlScript(self.dbOrigFileName, sql)

        # update with no changes in tabledefinition
        updater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        updater.update()

        rows = self.getTableRows( self.dbOrigFileName, "kurs" )
        self.assertEqual( 1, len(rows), "Table kurs should contain one row" )
        self.assertEqual( 2, len(rows[0]), "Table kurs 1st row should contain two cols" )

        rows = self.getTableRows( self.dbOrigFileName, "teilnehmer" )
        self.assertEqual( 1, len(rows), "Table teilnehmer should contain one row" )
        self.assertEqual( 3, len(rows[0]), "Table teilnehmer 1st row should contain three cols" )

    @unittest.skip("skipped temporarily")
    def test_RestoreByRowColStrategy(self):
        # add data
        sql  = 'INSERT INTO "kurs" VALUES(1, "Hüpfen");'
        sql += 'INSERT INTO "teilnehmer" VALUES(1, "Shwze", 1);'
        self.executeSqlScript(self.dbOrigFileName, sql)

        # add one col to teilnehmer
        tableColsSQL = self.tableColsSQL
        tableColsSQL['teilnehmer'].append( '"Vorname" VARCHAR(45)' )
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        rows = self.getTableRows( self.dbOrigFileName, "kurs" )
        self.assertEqual( 1, len(rows), "Table \"kurs\" should contain one row" )
        self.assertEqual( 2, len(rows[0]), "Table \"kurs\" 1st row should contain two cols" )

        rows = self.getTableRows( self.dbOrigFileName, "teilnehmer" )
        self.assertEqual( 1, len(rows), "Table \"teilnehmer\" should contain one row" )
        self.assertEqual( 4, len(rows[0]), "Table \"teilnehmer\" 1st row should contain four cols" )
        self.assertEqual( None, rows[0][3], "New col in table \"teilnehmer\" should contain no data" )

    def test_RestoreByRowColWithRenamedColStrategy(self):
        # add data
        sql  = 'INSERT INTO "kurs" VALUES(1, "Hüpfen");'
        sql += 'INSERT INTO "teilnehmer" VALUES(1, "Shwze", 1);'
        self.executeSqlScript(self.dbOrigFileName, sql)

        # change teilnehmer col name to Name
        tableColsSQL = self.tableColsSQL
        tableColsSQL['teilnehmer'][1] = '"Name" VARCHAR(45)'
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        rows = self.getTableRows( self.dbOrigFileName, "teilnehmer" )

        None

if __name__ == '__main__':
    unittest.main()
