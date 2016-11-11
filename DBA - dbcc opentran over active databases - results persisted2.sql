/* template to generate dynamic sql that loops over databases.
   You can restrict the DBs included in a variety of ways.

Input Required - Select the appropriate DB filters.
                 Fill in the text for the 'set @sql_text = '' 
                 Get the dynamic SQL to work.  Use @debug_only = 1 to print statements rather than execute.
                 Any place you need to use the name of the db, just use the variable @db_name.  logic uses 'replace' instead of concatenation, 
                     which lets the scripting get done much more easily.  example = 'select * from @db_name.sys.indexes'
                 If you have variables you need to pass into the dynamic sql, just extend the 'replace' logic.  You'll need to wrap the string variables
                  in (at least) 2 single quotes.  
                 In general, it tends to be simplest to write the SQL without the dynamic piece.  
                     Then, insert the SQL, replace all single quotes with 2 single quotes, place the variables, and test.  you will usually be pretty close.                           

common uses - remap users to logins when moving dbs to a new server.
               pull db-level permissions for a given login across all dbs.
               push a ddl trigger and associated table to all dbs on an instance for consistent data collection.
               calculate fragmentation for all indices across all dbs.

Notes     - tempdb is excluded whether selecting @user_dbs_only or @system_dbs_only.  this is intentional.  
            you wouldn't want to calculate fragmentation for tempdb tables, for example.  you can run for tempdb by entering
            the name in @db_include_list or running for @all_dbs = 1
          - collations can be a pain when running queries against multiple dbs, especially if you have multiple vendor dbs on the same instance.
            I make liberal use of 'COLLATE DATABASE_DEFAULT' in joins, variable declarations, etc.
          - @all_dbs = 1 is overridden when any of the other parameters are set.  

kill 548
Author - John Kauffman, http://sqljohnkauffman.wordpress.com/

*/

SET NOCOUNT ON


DECLARE @DEBUG_ONLY BIT = 0 -- BY DEFAULT, THE DYNAMIC SQL WILL BE PRINTED, NOT EXECUTED.
DECLARE @ALL_DBS BIT = 1
DECLARE @CURRENT_DB_ONLY BIT = 0
DECLARE @USER_DBS_ONLY BIT = 0
DECLARE @SYSTEM_DBS_ONLY BIT = 0
DECLARE @DB_INCLUDE_LIST NVARCHAR(1000) = '*'--'TMSDEV, FSSDev, ENTERPRISEWATCH'
DECLARE @DB_EXCLUDE_LIST NVARCHAR(1000) = '*'

IF OBJECT_ID(N'TEMPDB..#db_list') is not null DROP TABLE #db_list
IF OBJECT_ID(N'TEMPDB..#INCLUDED_DBS') is not null DROP TABLE #INCLUDED_DBS
IF OBJECT_ID(N'TEMPDB..#EXCLUDED_DBS') is not null DROP TABLE #EXCLUDED_DBS

create table #db_list (ROW_NUM int identity (1, 1), db_name sysname, db_id int)
create table #INCLUDED_DBS  ( DB_NAME SYSNAME )
create table #EXCLUDED_DBS ( DB_NAME SYSNAME)

/* DEAL WITH CSV LISTS*/
SET @DB_INCLUDE_LIST = upper(ltrim(rtrim(@DB_INCLUDE_LIST)))

IF @DB_INCLUDE_LIST IS NULL OR @DB_INCLUDE_LIST = ''  OR @DB_INCLUDE_LIST = 'ALL' OR @DB_INCLUDE_LIST = '*'  OR @DB_INCLUDE_LIST = 'NULL' 
      BEGIN  
         SET @DB_INCLUDE_LIST = '*'  
      END 

      IF @DB_INCLUDE_LIST <>'*'
         BEGIN
            INSERT INTO #INCLUDED_DBS
               SELECT upper(ltrim(rtrim(ELEMENT )))
               FROM MASTER.dbo.FN_SPLIT(@DB_INCLUDE_LIST, ',')
         END

SET @DB_EXCLUDE_LIST = upper(ltrim(rtrim(@DB_EXCLUDE_LIST)))

IF @DB_EXCLUDE_LIST IS NULL OR @DB_EXCLUDE_LIST = ''  OR @DB_EXCLUDE_LIST = 'ALL' OR @DB_EXCLUDE_LIST = '*'  OR @DB_EXCLUDE_LIST = 'NULL' 
      BEGIN  
         SET @DB_EXCLUDE_LIST = '*'  
      END 

      IF @DB_EXCLUDE_LIST <>'*'
         BEGIN
            INSERT INTO #EXCLUDED_DBS
               SELECT upper(ltrim(rtrim(ELEMENT )))
               FROM MASTER.dbo.FN_SPLIT(@DB_EXCLUDE_LIST, ',')
         END


insert into #db_list (db_name, db_id)
   select db.name, database_id
   from sys.databases db
   where state_desc = 'online'
   AND (@ALL_DBS = 1)
   AND (@CURRENT_DB_ONLY = 1 AND NAME = DB_NAME() OR @CURRENT_DB_ONLY = 0)
   AND ((@USER_DBS_ONLY = 1 AND NAME NOT IN ('MASTER', 'MODEL', 'MSDB', 'TEMPDB', 'DISTRIBUTION')) OR @USER_DBS_ONLY = 0)
   AND ((@SYSTEM_DBS_ONLY = 1 AND NAME  IN ('MASTER', 'MODEL', 'MSDB', 'DISTRIBUTION')) OR @SYSTEM_DBS_ONLY = 0)
   AND ((@DB_INCLUDE_LIST <> '*' AND NAME IN (SELECT DB_NAME FROM #INCLUDED_DBS)) OR @DB_INCLUDE_LIST = '*')
   AND ((@DB_EXCLUDE_LIST <> '*' AND NAME NOT IN (SELECT DB_NAME FROM #EXCLUDED_DBS)) OR @DB_EXCLUDE_LIST = '*')

IF OBJECT_ID(N'TEMPDB..#OpenTranStatus') is not null DROP TABLE #OpenTranStatus
IF OBJECT_ID(N'TEMPDB..#results') is not null DROP TABLE #results

CREATE TABLE #OpenTranStatus (
   Property varchar(25),
   Details sql_variant 
   )

create table #results (db_name sysname, spid int, transaction_start_datetime datetime, capture_datetime datetime)

IF OBJECT_ID(N'TEMPDB..#spid_info') is not null DROP TABLE #spid_info
CREATE TABLE #spid_info
(
 SESSION_ID smallint
,BLOCKING_SESSION_ID smallint
,STATUS nvarchar(30)
,SESSION_LOGIN_TIME datetime
,SESSION_TOTAL_ELAPSED_TIME int
,HOST_NAME nvarchar(128)
,PROGRAM_NAME nvarchar(128)
,LOGIN_NAME nvarchar(128)
,OBJECT_NAME nvarchar(128)
,SQL_STATEMENT nvarchar(MAX)
,SESSION_RESOURCES bigint
,REQ_START_TIME datetime
,REQ_TOTAL_ELAPSED_TIME int
,REQ_COMMAND nvarchar(1600)
,REQ_WAIT_TYPE nvarchar(60)
,REQ_WAIT_TIME int
,REQ_ROW_COUNT bigint
,REQUEST_RESOURCES bigint
,LAST_REQUEST_START_TIME datetime
,LAST_REQUEST_END_TIME datetime
,COMMAND varchar(5000)
)
------------------------------------------------------------------------------------
/* loop over DBs*/
declare @counter int = 1
declare @max_counter int = (SELECT MAX(ROW_NUM) FROM #DB_LIST)
declare @sql_text nvarchar(4000) = ''
DECLARE @DB_NAME SYSNAME
declare @db_id int
WHILE @COUNTER <= @MAX_COUNTER
   BEGIN
      SELECT @DB_NAME = DB_NAME, @db_id = db_id FROM #DB_LIST WHERE ROW_NUM = @COUNTER


      SET @SQL_TEXT = '
PRINT ''--  PROCESSING DATABASE @DB_NAME, @COUNTER OF @MAX_COUNTER   --''

  declare @spid int
  declare @sql_text nvarchar(1000)

  INSERT INTO #OpenTranStatus 
     EXEC (''DBCC OPENTRAN(@db_id) WITH TABLERESULTS, NO_INFOMSGS'');

   insert into #results 
      select ''@db_name''
      , (select cast(details as int) from #OpenTranStatus where Property = ''oldact_spid'') as spid
      , (select cast(details as datetime) from #OpenTranStatus where Property = ''OLDACT_STARTTIME'') as Start_datetime
      , getdate()
   
   truncate table #OpenTranStatus

         '

      SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@DB_NAME', @DB_NAME)
      SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@DB_id', @DB_id)
      SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@COUNTER', @COUNTER)
      SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@MAX_COUNTER', @MAX_COUNTER)

      PRINT (@SQL_TEXT)
      if @DEBUG_ONLY = 0 
         BEGIN
            EXEC  (@SQL_TEXT)
         END

      SET @COUNTER = @COUNTER + 1

END
declare @spid int
         select top 1 @spid = spid from #results where spid is not null

         --insert into #spid_info
         --exec dba.dbo.DBA_GetSessionInfoForSingleSPID @spid

IF OBJECT_ID(N'TEMPDB..#X') is not null DROP TABLE #X
CREATE TABLE #X
      (SESSION_ID                  SMALLINT      
      , BLOCKING_SESSION_ID        SMALLINT
      , STATUS                     NVARCHAR(30)
      , SESSION_LOGIN_TIME         DATETIME
      , SESSION_TOTAL_TIME_MS      INT
      , HOST_NAME                  NVARCHAR(128)
      , PROGRAM_NAME               NVARCHAR(128)
      , LOGIN_NAME                 NVARCHAR(128)
      , OBJECT_NAME                NVARCHAR(128)
      , SQL_STATEMENT              NVARCHAR(MAX)
      , SESSION_RESOURCES          BIGINT
      , REQ_START_TIME             DATETIME
      , REQ_TIME_MS                INT
      , REQ_COMMAND                NVARCHAR(1600)
      , REQ_WAIT_TYPE              NVARCHAR(60)
      , REQ_WAIT_TIME              INT
      , REQ_ROW_COUNT              BIGINT
      , REQUEST_RESOURCES          BIGINT
      , LAST_REQUEST_START_TIME    DATETIME
      , LAST_REQUEST_END_TIME      DATETIME

      )
/*PULL INFO FOR ALL TRANSACTIONS*/
INSERT INTO #X
SELECT S.SESSION_ID
   , R.BLOCKING_SESSION_ID
   , S.STATUS
   , S.LOGIN_TIME
   , S.TOTAL_ELAPSED_TIME      AS SESSION_TOTAL_ELAPSED_TIME
   , S.HOST_NAME
   , S.PROGRAM_NAME
   , S.LOGIN_NAME
   , O.NAME AS OBJECT_NAME
   ,     (SELECT TOP 1 SUBSTRING(S2.TEXT,STATEMENT_START_OFFSET / 2+1 , 
         ( (CASE WHEN STATEMENT_END_OFFSET = -1 
            THEN (LEN(CONVERT(NVARCHAR(MAX),S2.TEXT)) * 2) 
            ELSE STATEMENT_END_OFFSET END)  - STATEMENT_START_OFFSET) / 2+1))  AS SQL_STATEMENT
   , S.CPU_TIME + S.READS + S.LOGICAL_READS + S.WRITES AS SESSION_RESOURCES
   , R.START_TIME              AS REQ_START_TIME
   , R.TOTAL_ELAPSED_TIME      AS REQ_TOTAL_ELAPSED_TIME
   , R.COMMAND                 AS REQ_COMMAND
   , R.WAIT_TYPE               AS REQ_WAIT_TYPE
   , R.WAIT_TIME               AS REQ_WAIT_TIME
   , R.ROW_COUNT               AS REQ_ROW_COUNT
   , R.CPU_TIME + R.READS + R.LOGICAL_READS + R.WRITES AS REQUEST_RESOURCES
   , S.LAST_REQUEST_START_TIME
   , S.LAST_REQUEST_END_TIME

FROM SYS.DM_EXEC_SESSIONS                         AS  S
LEFT JOIN SYS.DM_EXEC_REQUESTS                    AS  R ON R.SESSION_ID = S.SESSION_ID
OUTER APPLY SYS.DM_EXEC_SQL_TEXT (SQL_HANDLE)     AS S2
LEFT JOIN SYS.OBJECTS                             AS  O ON O.OBJECT_ID = S2.OBJECTID
WhERE S.SESSION_ID in (select spid from #results)
---------------------------------------------------------------------------------------
/* PULLING INPUT BUFFER INFO*/

insert into #spid_info
select  X.*, ''
  from #x X


select r.spid
, r.db_name
, cast(datediff(second, transaction_start_datetime, getdate()) /60.0 as decimal(8, 2)) as minutes_open 
, datediff(second, transaction_start_datetime, getdate()) as seconds_open 
,transaction_start_datetime 
,capture_datetime 
,BLOCKING_SESSION_ID 
,STATUS
,SESSION_LOGIN_TIME 
,SESSION_TOTAL_ELAPSED_TIME 
,HOST_NAME
,PROGRAM_NAME 
,LOGIN_NAME 
,OBJECT_NAME
,SQL_STATEMENT 
,SESSION_RESOURCES 
,REQ_START_TIME 
,REQ_TOTAL_ELAPSED_TIME 
,REQ_COMMAND
,REQ_WAIT_TYPE 
,REQ_WAIT_TIME 
,REQ_ROW_COUNT 
,REQUEST_RESOURCES 
,LAST_REQUEST_START_TIME 
,LAST_REQUEST_END_TIME 
from #results r 
left join #spid_info s on s.session_id = r.spid
where spid is not null
order by transaction_start_datetime
