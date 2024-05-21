import os
import re
import sqlite3
import logging

if not 'ExportSQLiteError' in dir():
    ExportSQLiteError = ImportError

class SQLiteDbUpdater:
    # create update using path for database to update/create and sql script for creating
    def __init__(self, dbPath, createDbSql ) -> None:
        self.dbPath = dbPath
        self.createDbSql = createDbSql
        self.logger = None
        self.dbFileName = os.path.basename(self.dbPath)
        self.dbName = os.path.splitext(self.dbFileName)[0]
        self.dbTmpFileName = self.dbFileName + "~"
        self.dbRestoreFileName = self.dbName + "_restore.sql"
        self.dbDefinitionFileName =  self.dbName + "_definition.sql"
        self.confirmRequestCallback = None
        self.workDir = os.path.dirname( dbPath )
        self.logFile = os.path.join( self.workDir, self.dbName + ".log" )
        self.dbTableInfo = {}

    def log(self, msg, level=logging.INFO):
        if self.logger:
            logging.log( level, msg )

    def enableLogging(self):
        self.logger = logging.getLogger("SQLiteDbUpdater")
        logging.basicConfig(filename=self.logFile, filemode='wt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S')

    def getTableInfo(cursor, tableName):
        tableInfo = {}
        cursor.execute( "PRAGMA table_info(%s);" % tableName )
        return cursor.fetchall()
        
    # create database info to decide later howto dump/restore data
    def getDbTableInfo(dbFileName):
        dbTableInfo = {}
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='table'" )
            tableNames = cur.fetchall()
            for (tableName,) in tableNames:
                cur.execute( "select * from %s" % tableName )
                rows = cur.fetchall()
                dbTableInfo[tableName] = { 'columns': SQLiteDbUpdater.getTableInfo(cur, tableName), 'containsData' : len(rows) > 0 }
        finally:
            conn.close()
        
        return dbTableInfo

    # check if database already contains data
    def containsData(dbTableInfo):
        for tableName, tableInfo in dbTableInfo.items():
            if tableInfo['containsData']:
                return True
        return False

    def dumpTableByRow(tableRows, newTableName, file):
        for row in tableRows:
            sqlLine = 'INSERT INTO "%s" VALUES%s;' % (newTableName, row)
            file.write('%s\n' % sqlLine)

    def dumpTableByRowCol(tableRows, oldTableInfo, newTableName, newTableInfo, file):
        for row in tableRows:
            sqlColumnNames = []
            sqlColumnValues = []
            for idx,col in enumerate(oldTableInfo['columns']):
                sqlColumnNames.append( col[1] )
                if isinstance(row[idx], str):
                    sqlColumnValues.append( "\'" + row[idx] + "\'" )
                else:
                    sqlColumnValues.append( str(row[idx]) )
            
            sqlLine = 'INSERT INTO "%s"(%s) VALUES(%s)' % (newTableName, ','.join(sqlColumnNames), ','.join(sqlColumnValues) )
            file.write('%s\n' % sqlLine)

    # dump data of already existing database
    def dumpData(dbFileName, dbDumpFileName, dumpStrategy):
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            with open(dbDumpFileName, 'w') as f:
                cur.execute( "select name from sqlite_master where type='table'" )
                tableNames = cur.fetchall()
                for (tableName,) in tableNames:
                    strategy = dumpStrategy.get(tableName)
                    if strategy:
                        cur.execute( "select * from %s" % tableName )
                        rows = cur.fetchall()
                        strategy(rows, f)
        finally:                    
            conn.close()

    # restore dumped data to temporary created database
    def restoreData( dbFileName, dbDumpFileName ):
        with open(dbDumpFileName, 'rt') as f:
            sql = f.read()
            conn = sqlite3.connect(dbFileName)
            cur = conn.cursor()
            try:
                cur.executescript(sql)
                conn.commit()
            finally:
                cur.close()
                conn.close()

    # replace the dbname with the choosen filename stem                
    def substituteDbNameInSql(self):
        pattern = r"ATTACH \"([^ \"]+)\" AS \"([^ \";]+)\""
        match = re.search(pattern, self.createDbSql)
        if not match:
            raise ExportSQLiteError( 'Error', 'Cant evaluate/replace ATTACH ... line!')
        prevDbName = match.group(2)
        sql = re.sub(pattern, "ATTACH \"%s\" AS \"%s\"" %(self.dbTmpFileName,self.dbName), self.createDbSql)
        sql = re.sub( "\"" +  prevDbName + "\"\\.", "\"" + self.dbName + "\".", sql)
        return sql

    # stores sql creation script for inspection purposes, create backup of an already existing one
    def storeSql(sql, sqlFileName):
        sqlTmpFileName = sqlFileName + "~"

        if os.path.isfile(sqlTmpFileName):
            os.remove( sqlTmpFileName )

        if os.path.isfile(sqlFileName):
            os.rename( sqlFileName, sqlTmpFileName )

        with open(sqlFileName, 'w') as f:
            f.write(sql)

    def findTableByFingerprint(tableInfo, dbTableInfo):
        #for tableName, oldTableInfo in oldDbTableInfo.items():
        return None
    
    def evaluateDumpStrategy(self, oldDbTableInfo, newDbTableInfo):
        dumpStrategy = {}
        for tableName, oldTableInfo in oldDbTableInfo.items():
            if not oldDbTableInfo[tableName]['containsData']:
                continue
            newTableInfo = newDbTableInfo.get(tableName)
            newTableName = tableName
            if not newDbTableInfo:
                # check for renamed table
                newTableName = SQLiteDbUpdater.findTableByFingerprint(oldTableInfo, newDbTableInfo)
                if not newTableName:
                    info = "Table '%s' not found in new DB-schema, also not by column-fingerprint!" % tableName
                    info += "If table was renamed and also has changed colums try to rename it in the first run and change columns an a second run!"
                    self.log( info )
                    continue
                self.log( "Table '%s' was probably renamed, will try to restore data to table '%s!" % (tableName, newTableName) )
                newTableInfo = newDbTableInfo.get(newTableName)

            if oldTableInfo['columns'] != newTableInfo['columns']:
                self.log( "Table '%s' fingerprint has changed, maybe data will be not restored correctly!" % tableName )

            strategy = ""
            if len(oldTableInfo['columns']) == len(newTableInfo['columns']):
                dumpStrategy[tableName] = lambda tableRows, file, nameOfNewTable=newTableName : \
                    SQLiteDbUpdater.dumpTableByRow(tableRows, nameOfNewTable, file )
                strategy = "ByRow"
            else:
                dumpStrategy[tableName] = lambda tableRows, file, nameOfNewTable=newTableName : \
                    SQLiteDbUpdater.dumpTableByRowCol( tableRows, oldTableInfo, nameOfNewTable, newTableInfo, file )
                strategy = "ByRowCol"

            self.log( "Dump/Restore table \"%s\" by strategy: %s" % ( tableName, strategy ))

        return dumpStrategy

    # udpdate/create database in a most secure way
    # all updates changes will be made in a temporary created db
    # if all stuff went well, replace the current db with the temporary created one
    def update(self):
        self.log('Update started')
        os.chdir( self.workDir )

        # set choosen filename stem as db name in sql definition
        sql = self.substituteDbNameInSql()
        SQLiteDbUpdater.storeSql( sql, self.dbDefinitionFileName)

        # create db in dbTmpFileName
        if os.path.isfile(self.dbTmpFileName):
            os.remove( self.dbTmpFileName )
        conn = sqlite3.connect(self.dbTmpFileName)
        try:        
            cur = conn.cursor()
            cur.executescript(sql)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        # backup/restore data
        if os.path.isfile(self.dbFileName):
            oldDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbFileName )
            if SQLiteDbUpdater.containsData(oldDbTableInfo):
                newDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbTmpFileName )
                dumpStrategy = self.evaluateDumpStrategy(oldDbTableInfo, newDbTableInfo)
                SQLiteDbUpdater.dumpData(self.dbFileName, self.dbRestoreFileName, dumpStrategy)
                SQLiteDbUpdater.restoreData(self.dbTmpFileName, self.dbRestoreFileName)

        # on success replace dbFileName by dbTmpFileName
        if os.path.isfile(self.dbFileName):
            os.remove( self.dbFileName )
        os.rename( self.dbTmpFileName, self.dbFileName  )

        self.log('Update finished')

# test
if __name__ == '__main__':
    # load sql data
    dirName = os.path.dirname( __file__ )
    sql = ""
    sqlPath = dirName + "/test.sql"
    with open(sqlPath, 'rt') as f:
        sql = f.read()

    path = dirName + "/test.sqlite"
    upater = SQLiteDbUpdater(path, sql )
    upater.enableLogging()
    upater.update()
