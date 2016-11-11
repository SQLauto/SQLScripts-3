use master
go
CREATE PROCEDURE [dbo].[usp_GenTempTableHelperStatements] 
(
    @TABLE_NAME VARCHAR(MAX)
)
AS begin

/*
exec dbo.[usp_GenTempTableHelperStatements] '#TABLE'
*/

    SET NOCOUNT ON

    CREATE TABLE #TABLE
    (
        CREATE_STATEMENT NVARCHAR(MAX)
        ,COLUMN_ID        INT
    )


    CREATE TABLE #SELECT_COLUMNS
    (
        CREATE_STATEMENT NVARCHAR(MAX)
        ,COLUMN_ID        INT
    )

    CREATE TABLE #DECLARE
    (
        CREATE_STATEMENT NVARCHAR(MAX)
        ,COLUMN_ID        INT
    )

    CREATE TABLE #SELECT_DEFAULT_DATA_TYPE
    (
          CREATE_STATEMENT NVARCHAR(MAX)
        , COLUMN_ID        INT
        , COLUMN_NAME      NVARCHAR(500)
    )

    CREATE TABLE #SELECT_NULL
    (
          CREATE_STATEMENT NVARCHAR(MAX)
        , COLUMN_ID        INT
        , COLUMN_NAME      NVARCHAR(500)
    )
    -----------------------------------------------------------------------------------
   
    DECLARE @NUM_COLUMNS AS INT = 0, @COUNT INT, @I INT, @STMT NVARCHAR(MAX)

    SELECT   @NUM_COLUMNS = max_column_id_used 
    FROM     tempdb.sys.tables 
    WHERE    object_id = OBJECT_ID('tempdb.dbo.' + @TABLE_NAME)

    IF (@NUM_COLUMNS = 0)
    BEGIN
        print 'Temporary table "tempdb.dbo.' + @TABLE_NAME + '" not found.'
        return
    END
    -----------------------------------------------------------------------------------
    DECLARE @MAX_COL_NAME_LENGTH INT = 
      (SELECT MAX(LEN(NAME)) 
         From 
         tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME))
    -----------------------------------------------------------------------------------
   
    INSERT INTO #TABLE (CREATE_STATEMENT, COLUMN_ID)
         
        SELECT 
            'IF OBJECT_ID(N''TEMPDB..' + @TABLE_NAME + ''') IS NOT NULL DROP TABLE ' + @TABLE_NAME 
            ,1 AS column_id
        UNION ALL

        SELECT '', 2
      
        UNION ALL         
         
        SELECT 
            'CREATE TABLE ' + @TABLE_NAME 
            ,3 AS column_id
      
        UNION ALL
      
        SELECT 
            '('
            ,4
         
        UNION ALL

        SELECT
            CASE 
            WHEN Size IS NULL THEN
                CASE column_id
                    WHEN 1 THEN '      ' + COLUMN_NAME + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE 
                    ELSE '    , ' + COLUMN_NAME + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE
                END
            ELSE
                CASE column_id
                    WHEN 1 THEN '      ' + COLUMN_NAME + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')'                       
                    ELSE '    , ' + COLUMN_NAME + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' 
                END
            END AS CREATE_STATEMENT
            ,column_id + 4 AS column_id
        FROM
            (
            SELECT 
                A.name  AS COLUMN_NAME
            ,B.name  AS COLUMN_TYPE
            ,A.column_id 
            ,CASE                         
                WHEN B.name = 'nvarchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length/2 AS VARCHAR) END
                WHEN B.name = 'char' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'varchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'varbinary' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'binary' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'nchar' THEN CAST(A.max_length/2 AS VARCHAR)
                WHEN B.name = 'datetime2' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'datetimeoffset' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'time' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'float' THEN CAST(A.precision AS VARCHAR)
                WHEN B.name = 'decimal' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                WHEN B.name = 'numeric' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                ELSE NULL
                END AS Size 
            FROM
            (
            Select 
                * 
            From 
                tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME) 
            ) A
            JOIN sys.types B ON A.system_type_id = B.system_type_id AND B.name <> 'sysname'         
            )  sub_query

        UNION ALL
      
        SELECT 
            ')'
            ,@NUM_COLUMNS + 5
      
        ORDER BY 
            column_id 

 
    SELECT @COUNT = Count(*) FROM #TABLE
    SET @I = 1
    WHILE(@I <= @COUNT)
       BEGIN
           SELECT @STMT = CREATE_STATEMENT FROM #TABLE WHERE COLUMN_ID = @I
           print @STMT
           SET @I = @I + 1
       END
    PRINT '  '
    PRINT '-----------------------------------------------------------------------------'
    PRINT '  '



    INSERT INTO #SELECT_COLUMNS
         
        SELECT 
            'SELECT'
            ,1 AS column_id
      
        UNION ALL

        SELECT

         CASE column_id
            WHEN 1 THEN '      ' + COLUMN_NAME
            ELSE '    , ' + COLUMN_NAME
         END AS CREATE_STATEMENT
            ,column_id + 1 AS column_id
        FROM
            (
            SELECT 
                A.name  AS COLUMN_NAME
            ,A.column_id 
            FROM
            (
            Select 
                * 
            From 
                tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME) 
            ) A
            JOIN sys.types B ON A.system_type_id = B.system_type_id AND B.name <> 'sysname'         
            )  sub_query

       UNION ALL
       SELECT 'FROM ' + @TABLE_NAME, @NUM_COLUMNS + 2

        ORDER BY 
            column_id 

    SELECT @COUNT = Count(*) FROM #SELECT_COLUMNS
    SET @I = 1
    WHILE(@I <= @COUNT)
       BEGIN
           SELECT @STMT = CREATE_STATEMENT FROM #SELECT_COLUMNS WHERE COLUMN_ID = @I
           print @STMT
           SET @I = @I + 1
       END
    PRINT '  '
    PRINT '-----------------------------------------------------------------------------'
    PRINT '  '


    INSERT INTO #DECLARE
         
        SELECT 
            'DECLARE @' + Substring(@TABLE_NAME, 2, 99) + ' TABLE'
            ,1 AS column_id
      
        UNION ALL
      
        SELECT 
            '('
            ,2
         
        UNION ALL

        SELECT
            CASE 
            WHEN Size IS NULL THEN
                CASE column_id
                    WHEN 1 THEN '      ' + COLUMN_NAME  + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE 
                    ELSE '    , ' + COLUMN_NAME  + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE
                END
            ELSE
                CASE column_id
                    WHEN 1 THEN '      ' + COLUMN_NAME  + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')'                       
                    ELSE '    , ' + COLUMN_NAME  + REPLICATE(' ', @MAX_COL_NAME_LENGTH + 1 - LEN(COLUMN_NAME)) + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' 
                END
            END AS CREATE_STATEMENT
            ,column_id + 2 AS column_id
        FROM
            (
            SELECT 
                A.name  AS COLUMN_NAME
            ,B.name  AS COLUMN_TYPE
            ,A.column_id 
            ,CASE                         
                WHEN B.name = 'nvarchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length/2 AS VARCHAR) END
                WHEN B.name = 'char' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'varchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'varbinary' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'binary' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'nchar' THEN CAST(A.max_length/2 AS VARCHAR)
                WHEN B.name = 'datetime2' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'datetimeoffset' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'time' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'float' THEN CAST(A.precision AS VARCHAR)
                WHEN B.name = 'decimal' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                WHEN B.name = 'numeric' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                ELSE NULL
                END AS Size 
            FROM
            (
            Select 
                * 
            From 
                tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME) 
            ) A
            JOIN sys.types B ON A.system_type_id = B.system_type_id AND B.name <> 'sysname'         
            )  sub_query

        UNION ALL
      
        SELECT 
            ')'
            ,@NUM_COLUMNS + 3
      
        ORDER BY 
            column_id 

    SELECT @COUNT = Count(*) FROM #DECLARE
    SET @I = 1
    WHILE(@I <= @COUNT)
       BEGIN
           SELECT @STMT = CREATE_STATEMENT FROM #DECLARE WHERE COLUMN_ID = @I
           print @STMT
           SET @I = @I + 1
       END
    PRINT '  '
    PRINT '-----------------------------------------------------------------------------'
    PRINT '  '


DECLARE @MAX_COLUMN_TYPE_LENGTH INT 

    INSERT INTO #SELECT_DEFAULT_DATA_TYPE
         
        SELECT 
              'SELECT'
            , 1 AS column_id
            , null
      
        UNION ALL

        SELECT
            CASE 
            WHEN Size IS NULL THEN
                CASE column_id
                    WHEN 1 THEN '      Cast(' + DefVal + ' as ' + COLUMN_TYPE + ')'
                    ELSE '    , Cast(' + DefVal + ' as ' + COLUMN_TYPE + ')'
                END
            ELSE
                CASE column_id
                    WHEN 1 THEN '      Cast(' + DefVal + ' as ' + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' + ')'
                    ELSE '    , Cast(' + DefVal + ' as ' + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' + ')'
                END
            END AS CREATE_STATEMENT
            ,column_id + 1 AS column_id
            , ' as ' + COLUMN_NAME AS COLUMN_NAME
        FROM
            (
            SELECT 
                A.name  AS COLUMN_NAME
            ,B.name  AS COLUMN_TYPE
            ,A.column_id 
            ,CASE                         
                WHEN B.name = 'nvarchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length/2 AS VARCHAR) END
                WHEN B.name = 'char' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'varchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'varbinary' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'binary' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'nchar' THEN CAST(A.max_length/2 AS VARCHAR)
                WHEN B.name = 'datetime2' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'datetimeoffset' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'time' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'float' THEN CAST(A.precision AS VARCHAR)
                WHEN B.name = 'decimal' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                WHEN B.name = 'numeric' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                ELSE NULL
                END AS Size 
            ,CASE 
                WHEN b.name in ('bigint', 'int', 'smallint', 'tinyint', 'bit', 'float', 'decimal', 'numeric') THEN '0'
                WHEN b.name in ('datetime') THEN 'GetDate()'
                WHEN b.name in ('time') THEN '''00:00'''
                WHEN b.name in ('nvarchar', 'char', 'varchar', 'nchar') THEN ''''''
                ELSE 'NULL'
                END as DefVal
            FROM
            (
            Select 
                * 
            From 
                tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME) 
            ) A
            JOIN sys.types B ON A.system_type_id = B.system_type_id AND B.name <> 'sysname'         
            )  sub_query

        ORDER BY 
            column_id 

    set @MAX_COLUMN_TYPE_LENGTH  = (SELECT MAX(LEN(CREATE_STATEMENT)) FROM #SELECT_DEFAULT_DATA_TYPE)
   
    UPDATE #SELECT_DEFAULT_DATA_TYPE 
    SET CREATE_STATEMENT = CREATE_STATEMENT + REPLICATE(' ', @MAX_COLUMN_TYPE_LENGTH - LEN(CREATE_STATEMENT))  + COALESCE(COLUMN_NAME, '')

    SELECT @COUNT = Count(*) FROM #SELECT_DEFAULT_DATA_TYPE
    SET @I = 1
    WHILE(@I <= @COUNT)
       BEGIN
           SELECT @STMT = CREATE_STATEMENT FROM #SELECT_DEFAULT_DATA_TYPE WHERE COLUMN_ID = @I
           print @STMT
           SET @I = @I + 1
       END
    PRINT '  '
    PRINT '-----------------------------------------------------------------------------'
    PRINT '  '


    INSERT INTO #SELECT_NULL
         
        SELECT 
            'SELECT'
            ,1 AS column_id
            , NULL
      
        UNION ALL

        SELECT
            CASE 
            WHEN Size IS NULL THEN
                CASE column_id
                    WHEN 1 THEN '      Cast(' + DefVal + ' as ' + COLUMN_TYPE + ')' 
                    ELSE '    , Cast(' + DefVal + ' as ' + COLUMN_TYPE + ')'
                END
            ELSE
                CASE column_id
                    WHEN 1 THEN '      Cast(' + DefVal + ' as ' + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' + ')'
                    ELSE '    , Cast(' + DefVal + ' as ' + COLUMN_TYPE + '(' + CAST(size AS varchar) + ')' + ')'
                END
            END AS CREATE_STATEMENT
            ,column_id + 1 AS column_id
            , ' as ' + COLUMN_NAME AS COLUMN_NAME

        FROM
            (
            SELECT 
                A.name  AS COLUMN_NAME
            ,B.name  AS COLUMN_TYPE
            ,A.column_id 
            ,CASE                         
                WHEN B.name = 'nvarchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length/2 AS VARCHAR) END
                WHEN B.name = 'char' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'varchar' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'varbinary' THEN CASE WHEN A.max_length = -1 THEN 'MAX' ELSE CAST(A.max_length AS VARCHAR) END
                WHEN B.name = 'binary' THEN CAST(A.max_length AS VARCHAR)
                WHEN B.name = 'nchar' THEN CAST(A.max_length/2 AS VARCHAR)
                WHEN B.name = 'datetime2' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'datetimeoffset' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'time' THEN CAST(A.scale AS VARCHAR)
                WHEN B.name = 'float' THEN CAST(A.precision AS VARCHAR)
                WHEN B.name = 'decimal' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                WHEN B.name = 'numeric' THEN CAST(A.precision AS VARCHAR) + ',' + CAST(A.scale AS VARCHAR) 
                ELSE NULL
                END AS Size 
            ,'null' as DefVal
            FROM
            (
            Select 
                * 
            From 
                tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.' + @TABLE_NAME) 
            ) A
            JOIN sys.types B ON A.system_type_id = B.system_type_id AND B.name <> 'sysname'         
            )  sub_query

        ORDER BY 
            column_id 

   set @MAX_COLUMN_TYPE_LENGTH  = (SELECT MAX(LEN(CREATE_STATEMENT)) FROM #SELECT_NULL)
   
    UPDATE #SELECT_NULL 
    SET CREATE_STATEMENT = CREATE_STATEMENT + REPLICATE(' ', @MAX_COLUMN_TYPE_LENGTH - LEN(CREATE_STATEMENT))  + COALESCE(COLUMN_NAME, '')


    SELECT @COUNT = Count(*) FROM #SELECT_NULL
    SET @I = 1
    WHILE(@I <= @COUNT)
    BEGIN
        SELECT @STMT = CREATE_STATEMENT FROM #SELECT_NULL WHERE COLUMN_ID = @I
        print @STMT
        SET @I = @I + 1
    END


end

go
