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
            self.logger.log( level, msg )

    def enableLogging(self):
        self.logger = logging.getLogger("SQLiteDbUpdater")
        logging.basicConfig(filename=self.logFile, filemode='wt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S')
        return self.logger

    def getTableInfo(cursor, tableName):
        tableInfoByColName = {}
        tableInfoByColIdx = {}
        cursor.execute( "PRAGMA table_info(\"%s\");" % tableName )
        info = cursor.fetchall()
        for idx,col in enumerate(info):
            info = { 'cid': col[0], 'name': col[1], 'type': col[2], 'notnull': col[3], 'dflt_value': col[4], 'pk': col[5] }
            tableInfoByColIdx[idx] = info
            tableInfoByColName[col[1]] = info

        return tableInfoByColIdx, tableInfoByColName
            
    # create database info to decide later howto dump/restore data
    def getDbTableInfo(dbFileName):
        dbTableInfo = {}
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='table'" )
            tableNames = cur.fetchall()
            for (tableName,) in tableNames:
                cur.execute( "select * from \"%s\"" % tableName )
                rows = cur.fetchall()
                infoByColIdx, infoByColName = SQLiteDbUpdater.getTableInfo(cur, tableName)
                dbTableInfo[tableName] = { 'byIdx': infoByColIdx, 'byName': infoByColName, 'containsData' : len(rows) > 0 }
        finally:
            conn.close()
        
        return dbTableInfo
    
    # get fk names
    def getDbForeignIndexNames(dbFileName):
        dbForeignIndexNames = []
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='index'" )
            indexNames = cur.fetchall()
            for (indexName,) in indexNames:
                dbForeignIndexNames.append( indexName )
        finally:
            conn.close()
        
        return dbForeignIndexNames

    def getDbViewNames(dbFileName):
        dbViewNames = []
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='view'" )
            viewNames = cur.fetchall()
            for (viewName,) in viewNames:
                dbViewNames.append(viewName)
        finally:
            conn.close()
        
        return dbViewNames

    def getDbTriggerNames(dbFileName):
        dbTriggerNames = []
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='trigger'" )
            triggerNames = cur.fetchall()
            for (triggerName,) in triggerNames:
                dbTriggerNames.append(triggerName)
        finally:
            conn.close()
        
        return dbTriggerNames

    # check if database already contains data
    def containsData(dbTableInfo):
        for tableName, tableInfo in dbTableInfo.items():
            if tableInfo['containsData']:
                return True
        return False

    def restoreTableByRow(tableRows, newTableName, file):
        for row in tableRows:
            sqlLine = 'INSERT INTO "%s" VALUES%s;' % (newTableName, row)
            sqlLine = re.sub( "None", "NULL", sqlLine)
            file.write('%s\n' % sqlLine)

    def restoreTableByRowCol(tableRows, oldTableInfo, colNamesToRestore, newTableName, file):
        for row in tableRows:
            colNames = []
            values = []
            for colName in colNamesToRestore:
                colInfo = oldTableInfo['byName'][colName]
                idx = colInfo['cid']
                colNames.append( colName )
                if isinstance(row[idx], str):
                    values.append( "\'" + row[idx] + "\'" )
                else:
                    values.append( str(row[idx]) )
            
            sqlLine = 'INSERT INTO "%s"(%s) VALUES(%s);' % (newTableName, ','.join(colNames), ','.join(values) )
            sqlLine = re.sub( "None", "NULL", sqlLine)
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
    def substituteDbNameInSql(self, sql):
        pattern = r"ATTACH \"([^ \"]+)\" AS \"([^ \";]+)\""
        match = re.search(pattern, sql)
        if not match:
            raise ExportSQLiteError( 'Error', 'Cant evaluate/replace ATTACH ... line!')
        prevDbName = match.group(2)
        sql = re.sub(pattern, "ATTACH \"%s\" AS \"%s\"" %(self.dbTmpFileName,self.dbName), sql)
        sql = re.sub( "\"" +  prevDbName + "\"\\.", "\"" + self.dbName + "\".", sql)
        return sql

    # because we want to use it in MS Access, indexname should not contain '.'
    def fixIndexStatementsInSql(self, sql):
        # CREATE INDEX "mydb"."teilnehmer.fk_kursId_idx" ON "teilnehmer" ("fk_kursid");
        pattern = r'\nCREATE INDEX "([^"]+)"."([^\."]+)\.([^\."]+)" ON ([^\n]*)\n'
        repl = r'\nCREATE INDEX "\1"."\2_\3" ON \4\n'
        sql = re.sub( pattern, repl, sql )
        return sql

    def nameValid( self, name ):
        return re.search( "^[a-zA-Z+-_]*$", name ) != None

    # check tablenames, columnames for usable characters    
    def checkNames( self, dbTableInfo, dbForeignIndexNames, dbViewNames, dbTriggerNames ):
        for tableName, tableInfo in dbTableInfo.items():
            if not self.nameValid( tableName ):
                raise ExportSQLiteError( 'Error', 'Tablename "%s" contains not allowed characters!' % tableName )
            for colName, colInfo in tableInfo['byName'].items():
                if not self.nameValid( colName ):
                    raise ExportSQLiteError( 'Error', 'Columname "%s" of table "%s" contains not allowed characters!' % (colName, tableName) )
        for indexName in dbForeignIndexNames:
            if not self.nameValid( indexName ):
                raise ExportSQLiteError( 'Error', 'Indexname "%s" contains not allowed characters!' % indexName )
        for viewName in dbViewNames:
            if not self.nameValid( viewName ):
                raise ExportSQLiteError( 'Error', 'Viewname "%s" contains not allowed characters!' % viewName )
        for triggerName in dbTriggerNames:
            if not self.nameValid( triggerName ):
                raise ExportSQLiteError( 'Error', 'Triggername "%s" contains not allowed characters!' % triggerName )
    
    # stores sql creation script for inspection purposes, create backup of an already existing one
    def storeSql(sql, sqlFileName):
        sqlTmpFileName = sqlFileName + "~"

        if os.path.isfile(sqlTmpFileName):
            os.remove( sqlTmpFileName )

        if os.path.isfile(sqlFileName):
            os.rename( sqlFileName, sqlTmpFileName )

        with open(sqlFileName, 'w') as f:
            f.write(sql)

    def findTableByFingerprint(tableInfo, newDbTableInfo):
        for newTableName, newTableInfo in newDbTableInfo.items():
            if newTableInfo == tableInfo:
                return newTableName
        return None
    
    def evaluateRestoreStrategy(self, oldDbTableInfo, newDbTableInfo):
        restoreStrategy = {}
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

            strategy = ""
            # Case 1: no columndef changed
            if oldTableInfo['byIdx'] == newTableInfo['byIdx']:
                restoreStrategy[tableName] = lambda tableRows, file, nameOfNewTable=newTableName : \
                    SQLiteDbUpdater.restoreTableByRow(tableRows, nameOfNewTable, file )
                strategy = "RowByRow(No columns changed)"
            else:
                self.log( "Table '%s' fingerprint has been changed, maybe data will be not restored correctly!" % tableName, logging.WARN )
                # retrieving change info
                addedCols = []
                addedNotNullCols = []
                changedTypeCols = []
                changedToNotNullCols = []
                removedCols = []
                colNamesToRestore = []
                for name,colInfo in oldTableInfo['byName'].items():
                    if not name in newTableInfo['byName']:
                        removedCols.append( name )
                    else:
                        colNamesToRestore.append(name)
                        if newTableInfo['byName'][name]['type'] != colInfo['type']:
                            changedTypeCols.append(name)
                        elif newTableInfo['byName'][name]['notnull'] != colInfo['notnull'] and \
                             newTableInfo['byName'][name]['notnull'] == 1:
                            changedToNotNullCols.append(name)

                for name,colInfo in newTableInfo['byName'].items():
                    if not name in oldTableInfo['byName']:
                        addedCols.append( name )
                        if colInfo['notnull'] == 1:
                            addedNotNullCols.append(name)

                if len(changedToNotNullCols) or len(addedNotNullCols):
                    self.log( "Column(s) '%s' has been created/changed to have 'notNull' values, if restoring of data leads to problems, "
                              "start without 'notNull' in the first run, fill in data and then change definition to 'notNull' in the second run!"
                               % ','.join( addedNotNullCols + changedToNotNullCols ), logging.WARN )

                if len(changedTypeCols):
                    self.log( "Type of column(s) '%s' has been changed, if restoring of data leads to problems, "
                              "adapt data before change the datatype!" % ','.join( changedTypeCols ), logging.WARN )
                    
                # Case 2:
                # only col footprint changed, only added, only removed or only moved cols
                if (len(addedCols) * len(removedCols)) == 0:
                    restoreStrategy[tableName] = lambda tableRows, file, nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRowCol( tableRows, oldTableInfo, colNamesToRestore, nameOfNewTable, file )
                    strategy = "RowByNamedColumns(Columns added, columns removed or columns moved)"
                # Case 3:
                # check for renamed cols
                elif len(addedCols) == len(removedCols):
                    self.log( "Column(s) '%s' has been added and column(s) '%s' has been removed, this will be interpreted as changed col names!"
                              "If this is leads to problems, try to reorder, rename, remove or add only one column in a single run!"
                              % (','.join( addedCols ), ','.join( removedCols )), logging.WARN )
                    # check if unchanged column names stays at same index
                    movedCols = []
                    for nameToRestore in colNamesToRestore:
                        if newTableInfo['byName'][nameToRestore]['cid'] != oldTableInfo['byName'][nameToRestore]['cid']:
                            movedCols.append( nameToRestore )
                    # Case 3.1: ColumnNames has been renamed and moved -> Error
                    if len(movedCols):
                        self.log( "Column(s) '%s' has been moved to new positions!"
                                  "Restoring is not possible, try to reorder, rename, remove or add rows only in a single run!"
                                  % ','.join( movedCols ), logging.ERROR )
                        raise ExportSQLiteError( 'Error', 'Restoring is not possible for table: %s!' % tableName)

                    restoreStrategy[tableName] = lambda tableRows, file, nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRow(tableRows, nameOfNewTable, file )
                    strategy = "RowByRow(Columns renamed)"
                # Case 4: added and removed are not equal and both > 0 -> Error
                else:
                    self.log( "Column(s) '%s' has been added, this matches not the number of column(s) '%s' which has been removed!"
                              "Restoring is not possible, try to reorder, rename, remove or add rows only in a single run!"
                              % (','.join( addedCols ), ','.join( removedCols )), logging.ERROR )
                    raise ExportSQLiteError( 'Error', 'Restoring is not possible for table: %s!' % tableName)

            self.log( "Dump/Restore table \"%s\" by strategy: %s" % ( tableName, strategy ))

        return restoreStrategy

    # udpdate/create database in a most secure way
    # all updates changes will be made in a temporary created db
    # if all stuff went well, replace the current db with the temporary created one
    def update(self):
        self.log('Update started')
        os.chdir( self.workDir )

        sql = self.substituteDbNameInSql( self.createDbSql )
        sql = self.fixIndexStatementsInSql( sql )
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

        newDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbTmpFileName )
        newDbForeignIndexNames = SQLiteDbUpdater.getDbForeignIndexNames( self.dbTmpFileName )
        newDbViewNames = SQLiteDbUpdater.getDbViewNames( self.dbTmpFileName )
        newDbTriggerNames = SQLiteDbUpdater.getDbTriggerNames( self.dbTmpFileName )
        self.checkNames( newDbTableInfo, newDbForeignIndexNames, newDbViewNames, newDbTriggerNames )

        # backup/restore data
        if os.path.isfile(self.dbFileName):
            oldDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbFileName )
            if SQLiteDbUpdater.containsData(oldDbTableInfo):
                restoreStrategy = self.evaluateRestoreStrategy(oldDbTableInfo, newDbTableInfo)
                SQLiteDbUpdater.dumpData(self.dbFileName, self.dbRestoreFileName, restoreStrategy)
                SQLiteDbUpdater.restoreData(self.dbTmpFileName, self.dbRestoreFileName)

        # on success replace dbFileName by dbTmpFileName
        if os.path.isfile(self.dbFileName):
            os.remove( self.dbFileName )
        os.rename( self.dbTmpFileName, self.dbFileName  )

        self.log('Update finished')
