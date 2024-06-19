import sys
import os
import re
import unittest
import sqlite3
import copy

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
            'course': [
                '"id_course" INTEGER PRIMARY KEY NOT NULL',
                '"name" VARCHAR(45)' ],
            'participant': [
                '"id_participant" INTEGER PRIMARY KEY NOT NULL',
                '"name" VARCHAR(45)',
                '"course_id" INTEGER REFERENCES kurs (id_course)' # foreign key !!
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

        for tableName,colDefinition in tableColsSQL.items():
            sql += 'CREATE TABLE "test"."%s"(\n' % tableName
            sql += ',\n'.join( colDefinition )
            sql += ');\n'

        sql += 'CREATE INDEX "test"."participant.course_id_idx" ON "participant" ("course_id");\n'
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
        try:        
            conn = sqlite3.connect(dbFileName)
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            result = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        return result

    def getTableData(self, dbFileName, tableName ):
        try:
            os.chdir( self.workDir )
            conn = sqlite3.connect(dbFileName)
            cur = conn.cursor()
            cur.execute( "PRAGMA table_info(\"%s\");" % tableName )
            info = cur.fetchall()
        finally:
            conn.close()

        colNames = []
        for colInfo in info:
            colNames.append(colInfo[1])

        rows = self.executeSqlLine(dbFileName, "select * from \"%s\"" % tableName )
        tableData = []
        for row in rows:
            rowData = {}
            for colIdx,colName in enumerate(colNames):
                rowData[colName] = row[colIdx]
            tableData.append(rowData)

        return tableData
    
    def addSomeData( self, dbFileName ):
        courseData = [{
            'id_course':1,
            'name':'Jump'
        }]
        self.addTableData( dbFileName, 'course', courseData )

        participantData = [{
            'id_participant':1,
            'name':'Shwze',
            'course_id':1
        }]
        self.addTableData( dbFileName, 'participant', participantData )

        return courseData, participantData
    
    def addTableData( self, dbFileName, tableName, tableData ):
        colNames = []
        for key,value in tableData[0].items():
            colNames.append( key )

        sqlScript = ''
        for tableRow in tableData:
            values = []
            for key,value in tableRow.items():
                if isinstance(value, str):
                    values.append( "\'" + value + "\'" )
                else:
                    values.append( str(value) )
            sqlScript += 'INSERT INTO "%s"(%s) VALUES(%s);' % (tableName, ','.join(colNames), ','.join(values) )

        self.executeSqlScript(dbFileName, sqlScript)

    # Test test_substituteDbNameInSql with errornous userdata
    # @unittest.skip("skipped temporarily")
    def test_substituteDbNameInSql(self):
        sql = '-- Schema: Auftraggebersicht Wartungsplanung\n' +\
              'ATTACH "Auftraggebersicht Wartungsplanung.sdb" AS "Auftraggebersicht Wartungsplanung";\n' +\
              'BEGIN;\n'
        updater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        # should not raise an exception
        success = False
        try:
            updater.substituteDbNameInSql( sql )
            success = True
        except Exception as e:
            None

        self.assertTrue( success, "substituteDbNameInSql should work in this example" )

    # Test evaluateRestoreStrategy Case 1: RowByRow(No columns changed)
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByRowStrategy_no_columns_changed(self):
        courseOrigData, participantOrigData = self.addSomeData(self.dbOrigFileName)

        # update with no changes in tabledefinition
        updater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        updater.update()

        self.assertEqual( self.getTableData( self.dbOrigFileName, "course" ), courseOrigData,
                          "Course data should not change" )
        self.assertEqual( self.getTableData( self.dbOrigFileName, "participant" ), participantOrigData,
                         "Particpant data should not change" )

    # Test evaluateRestoreStrategy Case 2: RowByNamedColumns(Columns added, columns removed or columns moved)
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByNamedColumnsStrategy_columns_added(self):
        courseOrigData, participantOrigData = self.addSomeData(self.dbOrigFileName)

        # add one col to participant
        tableColsSQL = copy.deepcopy(self.tableColsSQL)
        tableColsSQL['participant'].append( '"Surname" VARCHAR(45)' )
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        self.assertEqual( self.getTableData( self.dbOrigFileName, "course" ), courseOrigData,
                         "Course data should not change" )

        participantData = self.getTableData( self.dbOrigFileName, "participant" )
        expectedParticipantData = copy.deepcopy( participantOrigData )
        expectedParticipantData[0]['Surname'] = None

        self.assertEqual( participantData, expectedParticipantData, "Participant should have one more column with None data" )


    # Test evaluateRestoreStrategy Case 2: RowByNamedColumns(Columns added, columns removed or columns moved)
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByNamedColumnsStrategy_columns_removed(self):
        courseOrigData, participantOrigData = self.addSomeData(self.dbOrigFileName)

        # add one col to participant
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['participant'].append( '"Surname" VARCHAR(45)' )
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        participantNewData = [{
            'id_participant':2,
            'name':'tom',
            'course_id': 1,
            'Surname':'Shwze'
        }]
        self.addTableData( self.dbOrigFileName, 'participant', participantNewData )

        participantData = self.getTableData( self.dbOrigFileName, "participant" )

        self.assertEqual( participantData[1], participantNewData[0], "Participant should have one more row/column with expected data" )

        # set old participant definition (without Surname col)
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        upater.update()

        expectedParticipantData = [{
            'id_participant':2,
            'name':'tom',
            'course_id': 1
        }]
        participantData = self.getTableData( self.dbOrigFileName, "participant" )
        self.assertEqual( participantData[1], expectedParticipantData[0], "Participant should have orig data" )

    # Test evaluateRestoreStrategy Case 2: RowByNamedColumns(Columns added, columns removed or columns moved)
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByNamedColumnsStrategy_columns_moved(self):
        courseOrigData, participantOrigData = self.addSomeData(self.dbOrigFileName)

        # reverse cols of participant
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        colsParticipant = tableColsSQL['participant']
        colsParticipant.reverse()
        tableColsSQL['participant'] = colsParticipant

        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        expectedParticipantData = [{
            'course_id': 1,
            'name':'Shwze',
            'id_participant':1,
        }]

        participantData = self.getTableData( self.dbOrigFileName, "participant" )
        self.assertNotEqual( str(participantData[0]), str(participantOrigData[0]), "Participant should have changed column order" )
        self.assertEqual( str(participantData[0]), str(expectedParticipantData[0]), "Participant should have expected new column order" )
        self.assertEqual( participantData[0], expectedParticipantData[0], "Participant should have expected new column order" )

    # Test evaluateRestoreStrategy Case 3: RowByRow(Columns renamed)
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByRowStrategy_columns_renamed(self):
        courseOrigData, participantOrigData = self.addSomeData(self.dbOrigFileName)

        # change participant col name to Name
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['participant'][1] = '"Name" VARCHAR(45)'
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        upater.update()

        expectedParticipantData = [{
            'id_participant':1,
            'Name':'Shwze',
            'course_id': 1,
        }]
        participantData = self.getTableData( self.dbOrigFileName, "participant" )

        self.assertEqual( participantData[0], expectedParticipantData[0], "Same data at renamed colummn expected" )

    # Test evaluateRestoreStrategy Case 3.1: ColumnNames has been renamed and moved -> Error
    # @unittest.skip("skipped temporarily")
    def test_RestoreRowByRowStrategy_columns_renamed_and_moved(self):
        self.addSomeData(self.dbOrigFileName)

        # change participant col name to Name
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['participant'][1] = '"Name" VARCHAR(45)'

        # reverse cols of participant
        colsParticipant = tableColsSQL['participant']
        colsParticipant.reverse()
        tableColsSQL['participant'] = colsParticipant

        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))

        with self.assertRaises( ImportError ):
            upater.update()

    # Test evaluateRestoreStrategy Case 4: added and removed are not equal and both > 0 -> Error
    # @unittest.skip("skipped temporarily")
    def test_Restore_different_count_of_rows_added_removed(self):
        self.addSomeData(self.dbOrigFileName)

        # change participant col name to Name -> 1 added 1 remove
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['participant'][1] = '"NewName" VARCHAR(45)'
        # add participant col -> 1 added ( in sum 2 added 1 removed)
        tableColsSQL['participant'].append( '"NewColumn" VARCHAR(45)' )
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        with self.assertRaises( ImportError ):
            upater.update()

    # @unittest.skip("skipped temporarily")
    def test_fixIndexStatementsInSql(self):
        creationSQL = self.getDbCreationSQL(self.tableColsSQL)

        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, creationSQL)
        upater.update()
        res = self.executeSqlLine(self.dbOrigFileName, "PRAGMA index_list(participant);")
        self.assertEqual( res[0][1], 'participant_course_id_idx', "No dots are allowed in index-names" )

        sql = '\nCREATE INDEX "W"."W.fk_W_W1_idx" ON "W" ("W_idW");\n'\
              'CREATE INDEX "WA"."W.fk_W_S1_idx" ON "W" ("S_idS");\n'

        updater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        res = re.findall( '\\.', sql )
        self.assertEqual( len(res), 4, "No dots are allowed in index-names" )

        sql = updater.fixIndexStatementsInSql( sql )
        res = re.findall( '\\.', sql )
        self.assertEqual( len(res), 2, "No dots are allowed in index-names" )

    # Test checkNames
    # @unittest.skip("skipped temporarily")
    def test_CheckNames(self):
        # should work without errors
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(self.tableColsSQL))
        upater.update()

        # test wrong tablename
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['wrong tablename'] = \
        [
            '"id" INTEGER PRIMARY KEY NOT NULL',
            '"name" VARCHAR(45)'
        ]
        
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        with self.assertRaises( ImportError ):
            upater.update()

        # test wrong colname
        tableColsSQL = copy.deepcopy( self.tableColsSQL )
        tableColsSQL['wrongCols1'] = \
        [
            '"id" INTEGER PRIMARY KEY NOT NULL',
            '"wrongüname" VARCHAR(45)'
        ]
        
        upater = SQLiteDbUpdater.SQLiteDbUpdater(self.dbOrigPath, self.getDbCreationSQL(tableColsSQL))
        with self.assertRaises( ImportError ):
            upater.update()

if __name__ == '__main__':
    unittest.main()
