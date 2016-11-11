
USE Master


SET NOCOUNT ON
--------------------------------------------------------------------------------------------------
/* SELECT OUTPUT*/
DECLARE @dATABASE_NAME           SYSNAME = '*'

/* SELECT SENSITIVITY*/
DECLARE @FILE_WARNING_PERCENT    INT = 20
DECLARE @FILE_CRITICAL_PERCENT   INT = 10

/* DEBUG PARMS*/
DECLARE @DEBUG_YN BIT = 1 -- WILL PRINT THE DYNAMIC SQL FOR REVIEW
DECLARE @EXEC_YN  BIT = 1 -- WILL EXECUTE THE DYNAMIC SQL.  FINAL RESULT SET IS NULL IF SET TO 0
----------------------------------------------------------------------------------------------------

DECLARE @SERVER_INSTANCE NVARCHAR(100) =  CAST(SERVERPROPERTY('SERVERNAME') AS NVARCHAR(100));

DECLARE @CAPTURE_ID INT
SELECT @CAPTURE_ID = 1--ISNULL(MAX(EVENT_ID), 0)+1 FROM DBA.DBO.DBA_SPACEUSED_BY_DB_BY_FILEGROUP

DECLARE @NOW DATETIME = GETDATE()


IF OBJECT_ID(N'TEMPDB..#DB_FILES') is not null DROP TABLE #DB_FILES
IF OBJECT_ID(N'TEMPDB..##SPACE_USED') is not null DROP TABLE ##SPACE_USED
IF OBJECT_ID(N'TEMPDB..##FILE_GROUPS') is not null DROP TABLE ##FILE_GROUPS
IF OBJECT_ID(N'TEMPDB..#VOLUME_SIZE') is not null DROP TABLE #VOLUME_SIZE
IF OBJECT_ID(N'TEMPDB..#FILE_SIZE') is not null DROP TABLE #FILE_SIZE

SELECT D.NAME              AS DB_NAME
   , D.DATABASE_ID         AS DB_ID
   , F.DATA_SPACE_ID       AS FILE_GROUP_ID
   , F.FILE_ID             AS FILE_ID
   , F.TYPE_DESC           AS FILE_TYPE_DESC
   , F.NAME                AS FILE_NAME
   , F.PHYSICAL_NAME       AS FILE_PHYSICAL_NAME
   , CAST(F.SIZE * 8/1024.0 AS DECIMAL(18,2)) AS FILE_CONFIGURED_MB
   , CAST(F.SIZE * 8/1024.0 AS DECIMAL(18,2)) AS FILE_CURRENT_MB
   , MAX_SIZE              AS FILE_MAX_SIZE
   , cast(GROWTH * 8/1024.0 as decimal(18, 2)) AS FILE_GROWTH_MB
   , IS_PERCENT_GROWTH     AS FILE_IS_PERCENT_GROWTH
   , ROW_NUMBER() OVER(ORDER BY D.NAME, F.TYPE_DESC DESC , F.NAME) AS ROW_NUM
   , QUOTENAME(d.name)     as db_quotename
INTO #DB_FILES
FROM SYS.MASTER_FILES F
JOIN SYS.DATABASES    D ON D.DATABASE_ID = F.DATABASE_ID
where d.state_desc= 'online'
AND (D.NAME = @dATABASE_NAME OR @dATABASE_NAME = '*')

USE TEMPDB
UPDATE DB 
SET FILE_CURRENT_MB =  CAST(F.SIZE * 8/1024.0 AS DECIMAL(18,2))
FROM #DB_FILES DB
JOIN SYS.DATABASE_FILES F ON F.FILE_ID = DB.FILE_ID
AND DB.DB_ID= 2



DECLARE @COUNTER INT = 1
DECLARE @MAX_COUNTER INT
SELECT @MAX_COUNTER = MAX(ROW_NUM) FROM #DB_FILES

DECLARE @DB_ID INT
DECLARE @DB_NAME SYSNAME
DECLARE @FILE_ID INT
DECLARE @FILE_NAME NVARCHAR(MAX)
DECLARE @FILE_GROUP_ID INT
DECLARE @SQL_TEXT NVARCHAR(MAX)

CREATE TABLE ##SPACE_USED (DB_NAME SYSNAME, FILE_ID INT, FILE_USED_MB DECIMAL(18, 2))
CREATE TABLE ##FILE_GROUPS (DB_NAME SYSNAME, FILE_ID INT, FILE_GROUP_ID INT, FILE_GROUP_NAME SYSNAME)

WHILE @COUNTER <= @MAX_COUNTER
BEGIN

   SELECT @DB_ID = DB_ID, @DB_NAME = DB_NAME, @FILE_ID = FILE_ID, @FILE_NAME = FILE_NAME, @FILE_GROUP_ID = FILE_GROUP_ID
   FROM #DB_FILES
   WHERE ROW_NUM = @COUNTER

   SET @SQL_TEXT = ''

   SET @SQL_TEXT = 'USE [@DB_NAME]
                    INSERT INTO ##SPACE_USED(DB_NAME, FILE_ID, FILE_USED_MB)
                       SELECT ''[' + @DB_NAME+ ']'', ' + CAST(@FILE_ID  AS NVARCHAR(10)) 
                  + ', CAST(FILEPROPERTY(''' + @FILE_NAME + ''',''SPACEUSED'') AS DECIMAL(18, 2))/8.00 /16.00

'

SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@DB_NAME', @DB_NAME)
SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@FILE_ID', @FILE_ID)
SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@FILE_NAME', @FILE_NAME) 

IF @DEBUG_YN = 1 
   PRINT @SQL_TEXT

IF @EXEC_YN = 1
   EXEC (@SQL_TEXT)

SET @COUNTER = @COUNTER + 1

END



SELECT 
     @CAPTURE_ID        AS CAPTURE_ID
   , CAST(@NOW AS DATE) AS CAPTURE_DATE
   , CAST(@NOW AS TIME) AS CAPTURE_TIME
   , @SERVER_INSTANCE   AS SERVER_INSTANCE
   ,  SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS SERVER

   , CASE WHEN FILE_PHYSICAL_NAME LIKE '%$%' THEN REPLACE(SUBSTRING(FILE_PHYSICAL_NAME, CHARINDEX('$', FILE_PHYSICAL_NAME) - 1,  CHARINDEX('\', FILE_PHYSICAL_NAME, CHARINDEX('$', FILE_PHYSICAL_NAME) + 3) - CHARINDEX('$', FILE_PHYSICAL_NAME) + 2 ) , '$', ':')
      ELSE SUBSTRING(FILE_PHYSICAL_NAME, 1,  CHARINDEX('\', FILE_PHYSICAL_NAME, 4)) END  AS VOLUME

   , F.DB_ID
   , F.DB_NAME                                                                          
   , CASE WHEN F.FILE_TYPE_DESC = 'LOG' THEN 'N/A - LOG' 
          ELSE CASE  FILEGROUP_NAME(F.FILE_GROUP_ID)
               WHEN 'PRIMARY' THEN '.PRIMARY' ELSE FILEGROUP_NAME(F.FILE_GROUP_ID)
               END
     END AS FILEGROUP_NAME
   , F.FILE_ID
   , F.FILE_NAME
   , F.FILE_TYPE_DESC AS FILE_TYPE
   , F.FILE_CONFIGURED_MB
   , F.FILE_CURRENT_MB
   , U.FILE_USED_MB
   , F.FILE_CONFIGURED_MB - U.FILE_USED_MB AS FILE_FREE_MB
   , CASE WHEN F.FILE_CONFIGURED_MB = 0 THEN 0 
          ELSE CAST(U.FILE_USED_MB/F.FILE_CONFIGURED_MB *100 AS DECIMAL(18, 2)) END AS FILE_USED_PCT
   , CASE WHEN F.FILE_CONFIGURED_MB = 0 THEN 0 
          ELSE CAST((F.FILE_CONFIGURED_MB - U.FILE_USED_MB)/F.FILE_CONFIGURED_MB *100 AS DECIMAL(18, 2)) END AS FILE_FREE_PCT
, CASE WHEN FILE_CONFIGURED_MB = 0 THEN 'N/A' 
       WHEN ROUND(CAST(F.FILE_CONFIGURED_MB - U.FILE_USED_MB AS FLOAT)/F.FILE_CONFIGURED_MB *100 , 2) <@FILE_CRITICAL_PERCENT THEN 'CRITICAL - ' + CAST(@FILE_CRITICAL_PERCENT AS VARCHAR(10)) + '% OR LESS FREE'
       WHEN ROUND(CAST(F.FILE_CONFIGURED_MB - U.FILE_USED_MB AS FLOAT)/F.FILE_CONFIGURED_MB *100 , 2) <@FILE_WARNING_PERCENT THEN 'WARNING - ' + CAST(@FILE_WARNING_PERCENT AS VARCHAR(10)) + '% OR LESS FREE' 
       ELSE 'OKAY' END AS FILE_STATUS

   , F.FILE_PHYSICAL_NAME
   , F.FILE_MAX_SIZE
   , F.FILE_GROWTH_MB
   , F.FILE_IS_PERCENT_GROWTH
FROM ##SPACE_USED U
JOIN #DB_FILES F ON F.DB_quoteNAME = U.DB_NAME 
                AND F.FILE_ID = U.FILE_ID
order by db_name, file_name



SET NOCOUNT OFF
