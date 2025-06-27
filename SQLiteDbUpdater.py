import os, re, sqlite3, logging, copy

if not 'ExportSQLiteError' in dir():
    ExportSQLiteError = ImportError

class ColInfo:
    def __init__(self, cid : int, name : str, type: str, notNull: bool, defaultValue: str, isPrimaryKey : bool):
        self.cid = cid
        self.name = name
        self.type = type
        self.notNull = notNull
        self.defaultValue = defaultValue
        self.isPrimaryKey = isPrimaryKey

    def diff(self, other : 'ColInfo' ) -> list[str]:
        diffList : list[str] = []
        if self.cid != other.cid:
            diffList.append(f'cid: {self.cid} <> {other.cid}')
        if self.name != other.name:
            diffList.append(f'name: "{self.name}" <> "{other.name}"')
        if self.type != other.type:
            diffList.append(f'type: "{self.type}" <> "{other.type}"')
        if self.notNull != other.notNull:
            diffList.append(f'notNull: {self.notNull} <> {other.notNull}')
        if self.defaultValue != other.defaultValue:
            diffList.append(f'defaultValue: "{self.defaultValue}" <> "{other.defaultValue}"')
        if self.isPrimaryKey != other.isPrimaryKey:
            diffList.append(f'isPrimaryKey: {self.isPrimaryKey} <> {other.isPrimaryKey}')
        return diffList

class TableInfo:
    def __init__(self, name : str, colInfoByIdx : dict[int, ColInfo], containsData : bool ):
        self.name = name
        self.containsData = containsData
        self.colInfoByIdx = colInfoByIdx
        self.colInfoByName : dict[str,ColInfo] = {}
        for idx,colInfo in sorted(self.colInfoByIdx.items()):
            self.colInfoByName[colInfo.name] = colInfo

    def diff(self, other : 'TableInfo' ) -> list[str]:
        diffList : list[str] = []
        if self.name != other.name:
            diffList.append(f'table name: "{self.name}" <> "{other.name}"')

        names = list(self.colInfoByName.keys())
        otherNames = list(other.colInfoByName.keys())
        allNames = list(otherNames)
        for name in names:
            if not name in allNames:
                allNames.append(name)

        for colName in allNames:
            if colName not in names:
                diffList.append(f'col "{colName}" not in table "{self.name}"')
            elif colName not in otherNames:
                diffList.append(f'col "{colName}" not in table "{other.name}"')
            else:
                diff = self.colInfoByName[colName].diff(other.colInfoByName[colName]) 
                if len(diff):
                    diffList.append(f'col "{colName}": {",".join(diff)}')
        return diffList

class SQLiteDbUpdater:
    # create update using path for database to update/create and sql script for creating
    def __init__(self, dbPath : str, createDbSql : str ) -> None:
        self.dbPath = dbPath
        self.createDbSql = createDbSql
        self.logger = None
        self.dbFileName = os.path.basename(self.dbPath)
        self.dbName = os.path.splitext(self.dbFileName)[0]
        self.dbTmpFileName = self.dbFileName + "~"
        self.dbRestoreDataFileName = self.dbName + "_restore.sql"
        self.dbRestoreViewsFileName = self.dbName + "_restoreViews.sql"
        self.dbOrigDefinitionFileName =  self.dbName + "_orig_definition.sql"
        self.dbDefinitionFileName =  self.dbName + "_definition.sql"
        self.confirmRequestCallback = None
        self.workDir = os.path.dirname( dbPath )
        self.logFile = os.path.join( self.workDir, self.dbName + ".log" )
        self.dbTableInfo = {}
        self.allowedCharacters = 'a-zA-Z0-9+-_ÄäÖöÜüß'

    def log(self, msg : str, level : int = logging.INFO):
        if self.logger:
            self.logger.log( level, msg )

    def enableLogging(self):
        self.logger = logging.getLogger("SQLiteDbUpdater")
        logging.basicConfig(filename=self.logFile, filemode='wt', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S')
        return self.logger
    
    @staticmethod
    def getTableInfo(cursor, tableName : str ):
        colInfoByIdx : dict[int,ColInfo] = {}
        cursor.execute( f'select * from "{tableName}"' )
        rows = cursor.fetchall()
        containsData = len(rows) > 0
        cursor.execute( f'PRAGMA table_info("{tableName}");')
        info = cursor.fetchall()
        for idx,col in enumerate(info):
            colInfo = ColInfo(col[0], col[1], col[2], col[3], col[4], col[5])
            colInfoByIdx[idx] = colInfo
        return TableInfo(tableName, colInfoByIdx, containsData)
            
    # create database info to decide later howto dump/restore data
    @staticmethod
    def getDbTableInfo(dbFileName : str ) -> dict[str,TableInfo]:
        dbTableInfo = {}
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            cur.execute( 'select name from sqlite_master where type="table"' )
            tableNames = cur.fetchall()
            for (tableName,) in tableNames:
                dbTableInfo[tableName] = SQLiteDbUpdater.getTableInfo(cur, tableName)
        finally:
            conn.close()
       
        return dbTableInfo
    
    # get fk names
    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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
    @staticmethod
    def containsData(dbTableInfo : dict[str,TableInfo]):
        for tableName, tableInfo in dbTableInfo.items():
            if tableInfo.containsData:
                return True
        return False

    def restoreTableByRow( self, tableRows, newTableName, file):
        sql = f'INSERT INTO "{newTableName}" VALUES\n'
        file.write(sql.encode('utf8'))

        sqlLines = []
        for row in tableRows:
            sqlLine = f'{row}'
            sqlLine = re.sub( "None", "NULL", sqlLine)
            sqlLines.append(sqlLine)

        file.write(',\n'.join(sqlLines).encode('utf8'))
        file.write(";\n".encode('utf8'))

    def restoreTableByRowCol(self, tableRows, oldTableInfo : TableInfo, colNamesToRestore, newTableName, file):
        oldColIdxByName = {} 
        for colName in colNamesToRestore:
            oldColIdxByName[colName] = oldTableInfo.colInfoByName[colName].cid

        quotedColNamesToRestore = []
        for colName in colNamesToRestore:
            quotedColNamesToRestore.append(f'"{colName}"')

        sql = f'INSERT INTO "{newTableName}"({",".join(quotedColNamesToRestore)}) VALUES\n'
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
            
            sqlLine = f'({",".join(values)})'
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
                cur.execute( 'select name from sqlite_master where type="table"' )
                tableNames = cur.fetchall()
                for (tableName,) in tableNames:
                    strategy = dumpStrategy.get(tableName)
                    if strategy:
                        cur.execute( f'select * from {tableName}' )
                        rows = cur.fetchall()
                        if len(rows):
                            strategy( self, rows, f )
        finally:                    
            conn.close()

    # restore dumped data to temporary created database
    @staticmethod
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
    def dumpViews(self, dbFileName, dbDumpFileName, renamingTableNames : dict[str,str],
                  renamingTableCols : dict[str,dict[str,str]]):
        conn = sqlite3.connect(dbFileName)
        try:
            cur = conn.cursor()
            with open(dbDumpFileName, 'wb') as file:
                cur.execute( "select name, sql from sqlite_master where type='view'" )
                views = cur.fetchall()
                for viewName,viewSql in views:
                    wrongChar = self.hasWrongCharacter( viewName )
                    if len(wrongChar) :
                        raise ExportSQLiteError( 'Error', f'View "{viewName}" contains not allowed character '\
                                                  f'"{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )

                    # treatment of renamed tables
                    if len(renamingTableNames):
                        for oldTableName,newTableName in renamingTableNames.items():
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
                    if len(renamingTableCols):
                        for tableName,colRenaming in renamingTableCols.items():
                            for oldColName,newColName in colRenaming.items():
                                pattern = r' +%s\.%s +' % (tableName, oldColName)
                                repl = r' %s.%s ' % (tableName, newColName)
                                viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )
                    
                    # treatment of viewnames with ' '
                    if ' ' in viewName:
                        pattern = f' +{viewName} +'
                        repl = f' [{viewName}] '
                        viewSql = re.sub( pattern, repl, viewSql, 0, re.IGNORECASE )

                    file.write((f'{viewSql};\n\n').encode('utf8'))
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
            except Exception as e:
                raise ExportSQLiteError( 'Error', f'Exception on restore views: {str(e)}' )
            finally:
                cur.close()
                conn.close()

    # replace the db-filename with the temp-db-filename
    def substituteDbNameInSql(self, sql):
        pattern = r"ATTACH \"([^\"]+)\""
        schemata = re.findall(pattern, sql)
        if len(schemata) > 1:
            raise ExportSQLiteError( 'Error',
                                     f'Only one schema per database allowed, but {len(schemata)} found ({schemata})!' )
        
        pattern = r"ATTACH \"([^\"]+)\" AS \"([^\";]+)\""
        match = re.search(pattern, sql)
        if match is None:
            raise ExportSQLiteError( 'Error', 'Cant find ATTACH pattern in SQL!' )
        prevDbName = match.group(2)
        sql = re.sub(pattern, f'ATTACH "{self.dbTmpFileName}" AS "{self.dbName}"', sql)
        sql = re.sub( "\"" +  prevDbName + "\"\\.", "\"" + self.dbName + "\".", sql)
        return sql

    # because we want to use it in MS Access, indexname should not contain '.'
    def fixIndexStatementsInSql(self, sql):
        # CREATE INDEX "mydb"."teilnehmer.fk_kursId_idx" ON "teilnehmer" ("fk_kursid");
        pattern = r'\nCREATE INDEX "([^"]+)"."([^\."]+)\.([^\."]+)" ON ([^\n]*)'
        repl = r'\nCREATE INDEX "\1"."\2_\3" ON \4'
        sql = re.sub( pattern, repl, sql )
        return sql

    # because we want to use it in MS Access, indexname should not contain '.'
    def changeDecimalToNumericInSql(self, sql):
        pattern = r' +DECIMAL\(([^\(\)]+)\)'
        repl = r' NUMERIC(\1)'
        sql = re.sub( pattern, repl, sql )

        pattern = r' +DECIMAL(,| |\))+'
        repl = r' NUMERIC(5,2)\1'
        sql = re.sub( pattern, repl, sql )
        return sql
    
    def hasWrongCharacter( self, name ):
        # regex will not find '/' and '.'
        if name.count('/') == 0 and name.count('.') == 0 \
           and re.search( f'^[{self.allowedCharacters}]*$', name ) != None:
            return ''

        for char in name:
            if re.match( f'[{self.allowedCharacters}]+', char ) == None \
                or char.count('/') > 0 or char.count('.') > 0:
                return char

        raise ExportSQLiteError( 'Error', 'Detected wrong charactar, but cant identify, should never appear!' )

    # check tablenames, columnames for usable characters    
    def checkNames( self, dbTableInfo : dict[str,TableInfo], dbForeignIndexNames, dbViewNames, dbTriggerNames ):
        for tableName, tableInfo in dbTableInfo.items():
            wrongChar = self.hasWrongCharacter( tableName )
            if len(wrongChar) :
                raise ExportSQLiteError( 'Error', f'Tablename "{tableName}" contains not allowed character '\
                                                  f'"{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )
            for colName, colInfo in tableInfo.colInfoByName.items():
                wrongChar = self.hasWrongCharacter( colName )
                if len(wrongChar) :
                    raise ExportSQLiteError( 'Error', f'Columname "{colName}" of table "{tableName}" contains not '\
                            f'allowed character "{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )
        for indexName in dbForeignIndexNames:
            wrongChar = self.hasWrongCharacter( indexName )
            if len(wrongChar) :
                raise ExportSQLiteError( 'Error', f'Indexname "{indexName}" contains not allowed character '\
                                                  f'"{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )
        for viewName in dbViewNames:
            wrongChar = self.hasWrongCharacter( viewName )
            if len(wrongChar) :
                raise ExportSQLiteError( 'Error', f'Viewname "{viewName}" contains not allowed character '\
                                                  f'"{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )
        for triggerName in dbTriggerNames:
            wrongChar = self.hasWrongCharacter( triggerName )
            if len(wrongChar) :
                raise ExportSQLiteError( 'Error', f'Triggername "{triggerName}" contains not allowed character '\
                                                  f'"{wrongChar}"! Allowed are: "{self.allowedCharacters}"' )
    
    # stores sql creation script for inspection purposes, create backup of an already existing one
    @staticmethod
    def storeSql(sql, sqlFileName):
        sqlTmpFileName = sqlFileName + "~"

        if os.path.isfile(sqlTmpFileName):
            os.remove( sqlTmpFileName )

        if os.path.isfile(sqlFileName):
            os.rename( sqlFileName, sqlTmpFileName )

        with open(sqlFileName, 'w') as f:
            f.write(sql)

    def findTableByFingerprint(self, tableInfo : TableInfo, newDbTableInfo : dict[str,TableInfo]) -> (str|None):
        colNames = list(tableInfo.colInfoByName.keys())
        for newTableName, newTableInfo in newDbTableInfo.items():
            newColNames = list(newTableInfo.colInfoByName.keys())
            if newColNames == colNames:
                return newTableName
        return None
    
    def evaluateRestoreStrategy(self, oldDbTableInfo : dict[str, TableInfo], newDbTableInfo : dict[str, TableInfo]):
        restoreStrategy = {}
        renamingTableNames : dict[str,str] = {}
        renamingTableCols : dict[str,dict[str,str]] = {}
        newTables = newDbTableInfo.keys()
        oldTables = oldDbTableInfo.keys()
        droppedTables : list[str] = []
        for oldTableName, oldTableInfo in oldDbTableInfo.items():
            newTableInfo = newDbTableInfo.get(oldTableName)
            if newTableInfo is None:
                # check for renamed table
                newTableName = self.findTableByFingerprint(oldTableInfo, newDbTableInfo)
                if not newTableName:
                    droppedTables.append(oldTableName)
                    continue
                self.log( f'Table "{oldTableName}" was probably renamed, will try to restore data to table '\
                          f'"{newTableName}"!')
                newTableInfo = newDbTableInfo.get(newTableName)
                renamingTableNames[oldTableName] = newTableName
            else:
                newTableName = oldTableName

            assert newTableInfo

            strategy = ""
            # Case 1: no columndef changed
            diffList = newTableInfo.diff(oldTableInfo)
            if not len(diffList):
                restoreStrategy[oldTableName] = lambda self, tableRows, file, nameOfNewTable=newTableName : \
                    SQLiteDbUpdater.restoreTableByRow( self, tableRows, nameOfNewTable, file )
                strategy = "RowByRow(No columns changed)"
            else:
                self.log( f'Table "{newTableName}" fingerprint has been changed ({",".join(diffList)}), '\
                           'maybe data will be not restored correctly!', logging.WARN )
                # retrieving change info
                addedCols = []
                addedNotNullCols = []
                changedTypeCols = []
                changedToNotNullCols = []
                removedCols = []
                colNamesToRestore = []
                for name,colInfo in oldTableInfo.colInfoByName.items():
                    if not name in newTableInfo.colInfoByName.keys():
                        removedCols.append( name )
                    else:
                        colNamesToRestore.append(name)
                        if newTableInfo.colInfoByName[name].type != colInfo.type:
                            changedTypeCols.append(name)
                        elif newTableInfo.colInfoByName[name].notNull != colInfo.notNull and \
                             newTableInfo.colInfoByName[name].notNull:
                            changedToNotNullCols.append(name)

                for name,colInfo in newTableInfo.colInfoByName.items():
                    if not name in oldTableInfo.colInfoByName.keys():
                        addedCols.append( name )
                        if colInfo.notNull:
                            addedNotNullCols.append(name)

                if len(changedToNotNullCols) or len(addedNotNullCols):
                    self.log( f'Column(s) "{",".join( addedNotNullCols + changedToNotNullCols )}" has been '\
                               'created/changed to have "notNull" values, if restoring of data leads to problems, '\
                               'start without "notNull" in the first run, fill in data and then change definition to '\
                               '"notNull" in the second run!', logging.WARN )

                if len(changedTypeCols):
                    self.log( f'Type of column(s) "{",".join( changedTypeCols )}" has been changed, if restoring of '\
                               'data leads to problems, adapt data before change the datatype!', logging.WARN )
                    
                # Case 2:
                # only col footprint changed, only added, only removed or only moved cols
                if (len(addedCols) * len(removedCols)) == 0:
                    restoreStrategy[oldTableName] = lambda self, tableRows, file, \
                        tableInfo=copy.deepcopy(oldTableInfo), colNames=colNamesToRestore, \
                        nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRowCol(
                            self, tableRows, tableInfo, colNames, nameOfNewTable, file )
                    strategy = "RowByNamedColumns(Columns added, columns removed or columns moved)"
                # Case 3:
                # check for renamed cols
                elif len(addedCols) == len(removedCols):
                    self.log( f'Column(s) "{",".join( addedCols )}" has been added and column(s) '\
                              f'"{",".join( removedCols )}" has been removed, this will be interpreted as changed col '\
                               'names! If this is leads to problems, try to reorder, rename, remove or add only one '\
                               'column in separate single runs!', logging.WARN )
                    # check if unchanged column names stays at same index
                    movedCols = []
                    for nameToRestore in colNamesToRestore:
                        if newTableInfo.colInfoByName[nameToRestore].cid != oldTableInfo.colInfoByName[nameToRestore].cid:
                            movedCols.append( nameToRestore )
                    # Case 3.1: ColumnNames has been renamed and moved -> Error
                    if len(movedCols):
                        self.log( f'Column(s) "{",".join( movedCols )}" has been moved to new positions! Restoring is '\
                                   'not possible, try to reorder, rename, remove or add rows in separate single runs!',
                                   logging.ERROR )
                        raise ExportSQLiteError( 'Error', f'Restoring is not possible for table: {oldTableName}!')

                    restoreStrategy[oldTableName] = lambda self, tableRows, file, nameOfNewTable=newTableName : \
                        SQLiteDbUpdater.restoreTableByRow( self, tableRows, nameOfNewTable, file )
                    strategy = "RowByRow(Columns renamed)"

                    # record renamings, for renaming in views
                    renamingCols : dict[str,str] = {}
                    for oldToNew in list(map(lambda x,y:(x,y), removedCols, addedCols)):
                        renamingCols[oldToNew[0]] = oldToNew[1]
                    renamingTableCols[newTableName] = renamingCols

                # Case 4: added and removed are not equal and both > 0 -> Error
                else:
                    self.log( f'Column(s) "{",".join( addedCols )}" has been added, this matches not the number of '\
                              f'column(s) "{",".join( removedCols )}" which has been removed! Restoring is not '\
                               'possible, try to reorder, rename, remove or add rows in separate single runs!',
                               logging.ERROR )
                    raise ExportSQLiteError( 'Error', f'Restoring is not possible for table: {oldTableName}!')

            self.log( f'Dump/Restore table "{oldTableName}" by strategy: {strategy}')

        if len(droppedTables) > 0 and len(newTables) == len(oldTables):
            err = f'Possibly renamed tables "{droppedTables}" could not be assigned to new names, also not by '\
                   'column-fingerprint! If tables were renamed and also have changed colums, try to rename tables one '\
                   'by one, each in a single run and later change columns in a second run!'
            self.log( err, logging.ERROR )
            raise ExportSQLiteError( 'Error', f'Restoring is not possible for tables: {droppedTables}!')

        return restoreStrategy,renamingTableNames,renamingTableCols

    # udpdate/create database in a most secure way
    # all updates changes will be made in a temporary created db
    # if all stuff went well, replace the current db with the temporary created one
    def update(self):
        self.log('Update started')
        os.chdir( self.workDir )

        self.log(f'Store original db definition sql file "{self.dbOrigDefinitionFileName}"' )
        SQLiteDbUpdater.storeSql( self.createDbSql, self.dbOrigDefinitionFileName)

        self.log('Substitute db name in sql')
        sql = self.substituteDbNameInSql( self.createDbSql )

        self.log('Fix index statements in sql')
        sql = self.fixIndexStatementsInSql( sql )

        self.log('Change DECIMAL to NUMERIC statements in sql')
        sql = self.changeDecimalToNumericInSql( sql )

        self.log(f'Store db updated/adapted creation sql file "{self.dbDefinitionFileName}"' )
        SQLiteDbUpdater.storeSql( sql, self.dbDefinitionFileName)

        # create db in dbTmpFileName
        self.log(f'Create db in temporary file "{self.dbTmpFileName}"' )
        if os.path.isfile(self.dbTmpFileName):
            os.remove( self.dbTmpFileName )
        conn = sqlite3.connect(self.dbTmpFileName)
        cur = None
        try:        
            cur = conn.cursor()
            cur.executescript(sql)
            conn.commit()
        finally:
            if cur:
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
            restoreStrategy,renamingTableNames,renamingTableCols = \
                self.evaluateRestoreStrategy(oldDbTableInfo, newDbTableInfo)
            if SQLiteDbUpdater.containsData(oldDbTableInfo):
                self.log(f'Backup and restore already existing db data for "{self.dbFileName}"')
                self.log(f'Dump db data to "{self.dbRestoreDataFileName}"' )
                self.dumpData(self.dbFileName, self.dbRestoreDataFileName, restoreStrategy)
                self.log(f'Restore db data from: "{self.dbRestoreDataFileName}" to temporary db "{self.dbTmpFileName}"')
                SQLiteDbUpdater.restoreData(self.dbTmpFileName, self.dbRestoreDataFileName)

            if SQLiteDbUpdater.containsViews(self.dbFileName):
                self.dumpViews(self.dbFileName, self.dbRestoreViewsFileName, renamingTableNames, renamingTableCols )
                self.restoreViews(self.dbTmpFileName, self.dbRestoreViewsFileName)

        # on success replace dbFileName by dbTmpFileName
        self.log(f'Move data from temporary db file "{self.dbTmpFileName}" to "{self.dbFileName}"')
        if os.path.isfile(self.dbFileName):
            os.remove( self.dbFileName )
        os.rename( self.dbTmpFileName, self.dbFileName  )

        self.log('Update finished')
