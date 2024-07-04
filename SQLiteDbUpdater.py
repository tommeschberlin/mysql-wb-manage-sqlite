import os
import re
import sqlite3
import logging
import copy

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
        self.dbRestoreDataFileName = self.dbName + "_restore.sql"
        self.dbRestoreViewsFileName = self.dbName + "_restoreViews.sql"
        self.dbDefinitionFileName =  self.dbName + "_definition.sql"
        self.confirmRequestCallback = None
        self.workDir = os.path.dirname( dbPath )
        self.logFile = os.path.join( self.workDir, self.dbName + ".log" )
        self.dbTableInfo = {}
        self.allowedCharacters = 'a-zA-Z0-9+-_'

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

    def containsViews(dbFileName):
        conn = sqlite3.connect(dbFileName)
        viewNames = []
        try:
            cur = conn.cursor()
            cur.execute( "select name from sqlite_master where type='view'" )
            viewNames = cur.fetchall()
        finally:
            conn.close()
        return len(viewNames) > 0

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

    def restoreTableByRow( self, tableRows, newTableName, file):
        sql = 'INSERT INTO "%s" VALUES\n' % newTableName
        file.write(sql.encode('utf8'))

        sqlLines = []
        for row in tableRows:
            sqlLine = '%s%s' % (row,'') # creapy formatting here, to get it work
            sqlLine = re.sub( "None", "NULL", sqlLine)
            sqlLines.append(sqlLine)

        file.write(',\n'.join(sqlLines).encode('utf8'))
        file.write(";\n".encode('utf8'))

    def restoreTableByRowCol(self, tableRows, oldTableInfo, colNamesToRestore, newTableName, file):
        oldColIdxByName = {} 
        for colName in colNamesToRestore:
            colInfo = oldTableInfo['byName'][colName]
            oldColIdxByName[colName] = colInfo['cid']

        sql = 'INSERT INTO "%s"(%s) VALUES\n' % (newTableName, ','.join(colNamesToRestore) )
        file.write(sql.encode('utf8'))

        sqlLines = []
        for row in tableRows:
            values = []
            for colName in colNamesToRestore:
                idx = oldColIdxByName[colName]
                # because of reordering we have treat different by type (strings and the other types)
                # in restoreTableByRow the list to string converter does this implicitely
                if isinstance(row[idx], str):
                    values.append( "\'" + row[idx] + "\'" )
                else:
                    values.append( str(row[idx]) )
            
            sqlLine = '(%s)' % ','.join(values)
            sqlLine = re.sub( "None", "NULL", sqlLine)
            sqlLines.append(sqlLine)

        file.write(',\n'.join(sqlLines).encode('utf8'))
        file.write(";\n".encode('utf8'))

    # dump data of already existing database
    def dumpData(self, dbFileName, dbDumpFileName, dumpStrategy):
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            with open(dbDumpFileName, 'wb') as f:
                cur.execute( "select name from sqlite_master where type='table'" )
                tableNames = cur.fetchall()
                for (tableName,) in tableNames:
                    strategy = dumpStrategy.get(tableName)
                    if strategy:
                        cur.execute( "select * from %s" % tableName )
                        rows = cur.fetchall()
                        strategy( self, rows, f)
        finally:                    
            conn.close()

    # restore dumped data to temporary created database
    def restoreData( dbFileName, dbDumpFileName ):
        with open(dbDumpFileName, 'rb') as f:
            sql = f.read().decode('utf8')
            conn = sqlite3.connect(dbFileName)
            cur = conn.cursor()
            try:
                cur.executescript(sql)
                conn.commit()
            finally:
                cur.close()
                conn.close()

    # dump views of already existing database
    def dumpViews(self, dbFileName, dbDumpFileName, renaming):
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            with open(dbDumpFileName, 'wb') as file:
                cur.execute( "select name, sql from sqlite_master where type='view'" )
                views = cur.fetchall()
                for viewName,viewSql in views:
                    # treatment of renamed tables
                    if 'tableNames' in renaming:
                        for oldTableName,newTableName in renaming['tableNames'].items():
                            pattern = r'FROM +%s +' % oldTableName
                            repl = r'FROM %s ' % newTableName
                            viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )

                            pattern = r'JOIN +%s +' % oldTableName
                            repl = r'JOIN %s ' % newTableName
                            viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )

                            pattern = r'([ \(")])%s\.' % oldTableName
                            repl = r'\1%s.' % newTableName
                            viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )

                    # treatment of renamed table cols
                    if 'columnNames' in renaming:
                        for tableName,colRenaming in renaming['columnNames'].items():
                            for oldColName,newColName in colRenaming.items():
                                pattern = r' +%s\.%s +' % (tableName, oldColName)
                                repl = r' %s.%s ' % (tableName, newColName)
                                viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )

                    file.write(('%s;\n\n' %viewSql).encode('utf8'))
        finally:                    
            conn.close()

    # restore dumped data to temporary created database
    def restoreViews( self, dbFileName, dbDumpFileName ):
        with open(dbDumpFileName, 'rb') as file:
            sql = file.read().decode('utf8')
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
        pattern = r"ATTACH \"([^\"]+)\" AS \"([^\";]+)\""
        match = re.search(pattern, sql)
        if match is None:
            raise ExportSQLiteError( 'Error', 'Cant find ATTACH pattern in SQL' )
        prevDbName = match.group(2)
        sql = re.sub(pattern, "ATTACH \"%s\" AS \"%s\"" %(self.dbTmpFileName,self.dbName), sql)
        sql = re.sub( "\"" +  prevDbName + "\"\\.", "\"" + self.dbName + "\".", sql)
        return sql

    # because we want to use it in MS Access, indexname should not contain '.'
    def fixIndexStatementsInSql(self, sql):
        # CREATE INDEX "mydb"."teilnehmer.fk_kursId_idx" ON "teilnehmer" ("fk_kursid");
        pattern = r'\nCREATE INDEX "([^"]+)"."([^\."]+)\.([^\."]+)" ON ([^\n]*)'
        repl = r'\nCREATE INDEX "\1"."\2_\3" ON \4'
        sql = re.sub( pattern, repl, sql )
        return sql

    def nameValid( self, name ):
        return re.search( "^[%s]*$" % self.allowedCharacters, name ) != None

    # check tablenames, columnames for usable characters    
    def checkNames( self, dbTableInfo, dbForeignIndexNames, dbViewNames, dbTriggerNames ):
        for tableName, tableInfo in dbTableInfo.items():
            if not self.nameValid( tableName ):
                raise ExportSQLiteError( 'Error', 'Tablename "%s" contains not allowed characters! Allowed are: "%s"'
                                         % ( tableName, self.allowedCharacters ) )
            for colName, colInfo in tableInfo['byName'].items():
                if not self.nameValid( colName ):
                    raise ExportSQLiteError( 'Error', 'Columname "%s" of table "%s" contains not allowed characters! Allowed are: "%s"'
                                             % (colName, tableName, self.allowedCharacters) )
        for indexName in dbForeignIndexNames:
            if not self.nameValid( indexName ):
                raise ExportSQLiteError( 'Error', 'Indexname "%s" contains not allowed characters! Allowed are: "%s"'
                                         % (indexName, self.allowedCharacters) )
        for viewName in dbViewNames:
            if not self.nameValid( viewName ):
                raise ExportSQLiteError( 'Error', 'Viewname "%s" contains not allowed characters! Allowed are: "%s"'
                                         % (viewName, self.allowedCharacters) )
        for triggerName in dbTriggerNames:
            if not self.nameValid( triggerName ):
                raise ExportSQLiteError( 'Error', 'Triggername "%s" contains not allowed characters! Allowed are: "%s"'
                                         % (triggerName, self.allowedCharacters) )
    
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
        renaming = {}
        for oldTableName, oldTableInfo in oldDbTableInfo.items():
            newTableInfo = newDbTableInfo.get(oldTableName)
            if newTableInfo is None:
                # check for renamed table
                newTableName = SQLiteDbUpdater.findTableByFingerprint(oldTableInfo, newDbTableInfo)
                if newTableName is None:
                    info = "Table '%s' not found in new DB-schema, also not by column-fingerprint!" % oldTableName
                    info += "If table was renamed and also has changed colums try to rename it in the first run and change columns an a second run!"
                    self.log( info, logging.WARN )
                    continue
                self.log( "Table '%s' was probably renamed, will try to restore data to table '%s!" % (oldTableName, newTableName) )
                newTableInfo = newDbTableInfo.get(newTableName)
                renaming['tableNames'] = { oldTableName : newTableName }
            else:
                newTableName = oldTableName

            strategy = ""
            # Case 1: no columndef changed
            if oldTableInfo['byIdx'] == newTableInfo['byIdx']:
                restoreStrategy[oldTableName] = lambda self, tableRows, file, nameOfNewTable=newTableName : \
                    SQLiteDbUpdater.restoreTableByRow( self, tableRows, nameOfNewTable, file )
                strategy = "RowByRow(No columns changed)"
            else:
                self.log( "Table '%s' fingerprint has been changed, maybe data will be not restored correctly!" % oldTableName, logging.WARN )
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
                    copiedOldTableInfo = copy.deepcopy( oldTableInfo )
                    restoreStrategy[oldTableName] = lambda self, tableRows, file, nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRowCol( self, tableRows, copiedOldTableInfo, colNamesToRestore, nameOfNewTable, file )
                    strategy = "RowByNamedColumns(Columns added, columns removed or columns moved)"
                # Case 3:
                # check for renamed cols
                elif len(addedCols) == len(removedCols):
                    self.log( "Column(s) '%s' has been added and column(s) '%s' has been removed, this will be interpreted as changed col names!"
                              "If this is leads to problems, try to reorder, rename, remove or add only one column in separate single runs!"
                              % (','.join( addedCols ), ','.join( removedCols )), logging.WARN )
                    # check if unchanged column names stays at same index
                    movedCols = []
                    for nameToRestore in colNamesToRestore:
                        if newTableInfo['byName'][nameToRestore]['cid'] != oldTableInfo['byName'][nameToRestore]['cid']:
                            movedCols.append( nameToRestore )
                    # Case 3.1: ColumnNames has been renamed and moved -> Error
                    if len(movedCols):
                        self.log( "Column(s) '%s' has been moved to new positions!"
                                  "Restoring is not possible, try to reorder, rename, remove or add rows in separate single runs!"
                                  % ','.join( movedCols ), logging.ERROR )
                        raise ExportSQLiteError( 'Error', 'Restoring is not possible for table: %s!' % oldTableName)

                    restoreStrategy[oldTableName] = lambda self, tableRows, file, nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRow( self, tableRows, nameOfNewTable, file )
                    strategy = "RowByRow(Columns renamed)"

                    # record renamings, for renaming in views
                    renamingCols = {}
                    for oldToNew in list(map(lambda x,y:(x,y), removedCols, addedCols)):
                        renamingCols[oldToNew[0]] = oldToNew[1]
                    renaming['columnNames'] = { newTableName : renamingCols }

                # Case 4: added and removed are not equal and both > 0 -> Error
                else:
                    self.log( "Column(s) '%s' has been added, this matches not the number of column(s) '%s' which has been removed!"
                              "Restoring is not possible, try to reorder, rename, remove or add rows in separate single runs!"
                              % (','.join( addedCols ), ','.join( removedCols )), logging.ERROR )
                    raise ExportSQLiteError( 'Error', 'Restoring is not possible for table: %s!' % oldTableName)

            self.log( "Dump/Restore table \"%s\" by strategy: %s" % ( oldTableName, strategy ))

        return restoreStrategy,renaming

    # udpdate/create database in a most secure way
    # all updates changes will be made in a temporary created db
    # if all stuff went well, replace the current db with the temporary created one
    def update(self):
        self.log('Update started')
        os.chdir( self.workDir )

        self.log('Substitute db name in sql')
        sql = self.substituteDbNameInSql( self.createDbSql )

        self.log('Fix index statements in sql')
        sql = self.fixIndexStatementsInSql( sql )

        self.log('Store db creation sql file "%s"' % self.dbDefinitionFileName )
        SQLiteDbUpdater.storeSql( sql, self.dbDefinitionFileName)

        # create db in dbTmpFileName
        self.log('Create db in temporary file "%s"' % self.dbTmpFileName )
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

        self.log( 'Retrieve new table/index/view/trigger info' )
        newDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbTmpFileName )
        newDbForeignIndexNames = SQLiteDbUpdater.getDbForeignIndexNames( self.dbTmpFileName )
        newDbViewNames = SQLiteDbUpdater.getDbViewNames( self.dbTmpFileName )
        newDbTriggerNames = SQLiteDbUpdater.getDbTriggerNames( self.dbTmpFileName )

        self.log( 'Check new table/index/view/trigger names' )
        self.checkNames( newDbTableInfo, newDbForeignIndexNames, newDbViewNames, newDbTriggerNames )

        # backup/restore data
        if os.path.isfile(self.dbFileName):
            self.log( 'Retrieve old table info' )
            oldDbTableInfo = SQLiteDbUpdater.getDbTableInfo( self.dbFileName )
            self.log( 'Evaluate restore strategy for tables' )
            restoreStrategy,renaming = self.evaluateRestoreStrategy(oldDbTableInfo, newDbTableInfo)
            if SQLiteDbUpdater.containsData(oldDbTableInfo):
                self.log( 'Backup and restore already existing db data for "%s"' % self.dbFileName )
                self.log( 'Dump db data to "%s"' % self.dbRestoreDataFileName )
                self.dumpData(self.dbFileName, self.dbRestoreDataFileName, restoreStrategy)
                self.log( 'Restore db data from: "%s" to temporary db "%s"' % (self.dbRestoreDataFileName, self.dbTmpFileName) )
                SQLiteDbUpdater.restoreData(self.dbTmpFileName, self.dbRestoreDataFileName)

            if SQLiteDbUpdater.containsViews(self.dbFileName):
                self.dumpViews(self.dbFileName, self.dbRestoreViewsFileName, renaming )
                self.restoreViews(self.dbTmpFileName, self.dbRestoreViewsFileName)

        # on success replace dbFileName by dbTmpFileName
        self.log('Move data from temporary db file "%s" to "%s"' % (self.dbTmpFileName, self.dbFileName ))
        if os.path.isfile(self.dbFileName):
            os.remove( self.dbFileName )
        os.rename( self.dbTmpFileName, self.dbFileName  )

        self.log('Update finished')
