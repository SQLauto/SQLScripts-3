/* 
SCRIPT TO GET VARIOUS TEMPDB-ORIENTED OUTPUTS.  
EACH OPTION IS A SEPARATE OUTPUT, THOUGH SESSION-LEVEL OUTPUTS CAN OPTIONALLY BE COMBINED.

AUTHOR:  JOHN KAUFFMAN, THE JOLLY DBA

KNOWN ISSUES:  
   LONG-LASTING LOCKS ON SYS.OBJECTS CAN CAUSE CERTAIN OUTPUTS TO HANG (e.g. @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES)
   BECAUSE OF THE OPTIONS RELATED TO SESSIONS, I COLLECT DATA IN 3 SEPARATE PULLS.  IN HIGH VOLUME SYSTEMS, MIS-ALIGNMENTS CAN HAPPEN.  
   UPPER CASE MAY CAUSE ISSUES WITH CASE-SENSITIVE COLLATIONS.

Disclaimer:
This Sample Code is provided for the purpose of illustration only and is not intended
to be used in a production environment.  THIS SAMPLE CODE AND ANY RELATED INFORMATION ARE
PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR
PURPOSE.  We grant You a nonexclusive, royalty-free right to use and modify the Sample Code
and to reproduce and distribute the object code form of the Sample Code, provided that You
agree: 
(i) to not use Our name, logo, or trademarks to market Your software product in which
the Sample Code is embedded; 
(ii) to include a valid copyright notice on Your software product
in which the Sample Code is embedded; 
and (iii) to indemnify, hold harmless, and defend Us and
Our suppliers from and against any claims or lawsuits, including attorneys fees, that arise or
result from the use or distribution of the Sample Code.
*/


/* version store detailed analyis not done yet.  
dmvs to consider include
      sys.dm_tran_version_store
      sys.dm_tran_top_version_generators
      sys.dm_tran_current_transaction
      sys.dm_tran_active_transactions
      sys.dm_tran_transactions_snapshot
      sys.dm_tran_session_transactions
      sys.dm_tran_database_transactions
      sys.dm_tran_current_snapshot
*/

SET DEADLOCK_PRIORITY LOW

USE TEMPDB

/* CONFIG REVIEW */
DECLARE @SHOW_CONFIG_BEST_PRACTICES            BIT = 1
DECLARE @SHOW_FILE_UTILIZATION_BY_FUNCTION     BIT = 1    -- SPACE USED BY INTERNAL OBJECTS, VERSION STORE, ETC.
DECLARE @SHOW_FILE_SPACE                       BIT = 1    -- LISTS CONFIGURED AND CONSUMED SPACE BY FILE, PLUS AUTOGROWTH SETTINGS.                                                  

/* PERFORMANCE*/

DECLARE @SHOW_PERFMON_COUNTERS                 BIT = 1     -- OUTPUT COLLECTS 2 SETS OF DATA AND CALCULATES DIFFS, BASED ON NEXT PARM.
DECLARE @PERFMON_COUNTER_COLLECTION_TIME_SECS  INT = 1     

DECLARE @SHOW_HISTORICAL_IO                    BIT = 0 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.
DECLARE @SHOW_CURRENT_IO                       BIT = 1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @IO_CAPTURE_SECONDS                    INT = 2

/* PROBLEMS */
DECLARE @SHOW_OLDEST_TRANSACTION               BIT = 1
DECLARE @SHOW_LOG_INFO                         BIT = 1

DECLARE @SHOW_PAGE_ALLOCATION_CONTENTION       BIT = 1
DECLARE @PAGE_ALLOCATION_CONTENTION_LOOPS      INT = 3
DECLARE @PAGE_ALLOCATION_CONTENTION_SECS_PER   INT = 1

DECLARE @INCLUDE_SESSION_INFO                  BIT = 1     -- ADD INFO FROM SYS.SESSIONS / SYS.REQUESTS  TO THE SESSION-LEVEL OUTPUTS.  
DECLARE @SESSION_ID                            INT = NULL  -- NULL FOR ALL, OR FILTER BY SPID

DECLARE @SHOW_LOCKS_IN_TEMPDB                  BIT = 1

/* USER TABLE OUTPUT, WITH OPTION.  NOT FREQUENTLY USED, SINCE YOU CANNOT TIE BACK TO SESSIONS*/
DECLARE @SHOW_USER_TABLES_IN_TEMPDB            BIT = 0     -- WOULD BE GREAT TO LINK TO SESSIONS GENERATING THEM. MAYBE SOMEDAY...
DECLARE @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES BIT = 0     -- NOT SPEEDY IF TEMPDB IS LARGE!  BLOCKED BY OPEN TRANSACTIONS
DECLARE @INCLUDE_CACHED_VS_SPOOLED_PAGES       BIT = 0     -- CAN BE RUN WITH OR WITHOUT FILE BREAKOUT.  NOT SPEEDY IF DATA CACHE IS LARGE!
DECLARE @SHOW_DETAIL                           BIT = 1
DECLARE @SHOW_SUMMARY                          BIT = 1     -- GROUP BY OBJECT_TYPE, FILE (IF FILE BREAKOUT SELECTED)
DECLARE @INCLUDE_INTERNAL_TABLES               BIT = 0
DECLARE @INCLUDE_SYSTEM_TABLES                 BIT = 0


------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------
/* CONFIGURATIONS - NUMBER OF FILES VS NUMBER OF CPUS; DATA FILES OF EQUAL SIZE; TRACE -1118 ON (NO MIXED EXTENTS)*/
------------------------------------------------------------------------------------------------------------

IF @SHOW_CONFIG_BEST_PRACTICES = 1
BEGIN

   DECLARE	@cpu_count int
          , @file_count int
   set @cpu_count=0
   set @file_count =0


   SELECT	@cpu_count = cpu_count 
   FROM	sys.dm_os_sys_info

   SELECT	@file_count = COUNT(name)
   FROM	tempdb.sys.database_files
   WHERE	type = 0
IF OBJECT_ID(N'TEMPDB..#config') is not null DROP TABLE #config
   CREATE TABLE #config (CATEGORY NVARCHAR (100), MSG NVARCHAR(1024), BEST_PRACTICE NVARCHAR(1024), IMPACT NVARCHAR(1024))
   INSERT INTO #config SELECT N'TempDB data files vs physical CPUs', N'No action needed - Data files match CPU cores.', N'Create as many tempdb data files (equally sized) as cores, up to 8.  After, increase by sets of 4 if needed.', N'Helps resolve page allocation contention.  Not needed if problem does not exist.'
   INSERT INTO #config SELECT N'TempDB data files are equally sized.', N'No action needed.  Data files are equally sized.', N'Create equally sized TempDB data files.', N'Matching sizes support even use across files, minimizing hot spots on a single larger file.'
   INSERT INTO #config SELECT N'Trace 1118 (no mixed extents) enabled', N'No action needed.  Trace 1118 Enabled.  Mixed extents not used in TempDB.', N'Set 1118 as start up trace.', N'Mixed extents increase contention for shared resources. Note that disabling mixed extents may cause some TempDB growth.  Note that some mixed extents will still appear, because TempDB is recreated from Model, which was created before 1118 is enabled.'


   IF NOT (@file_count % @cpu_count = 0) and @file_count < 8
   BEGIN
	   update #config set msg =   N'Action may be appropriate.  CPU count <> FIlE count, CPU count ' + CONVERT(NVARCHAR(10), @cpu_count) + 
             N' FILE count = ' + CONVERT(NVARCHAR(10), @file_count)
      where category = N'TempDB data files vs physical CPUs'
   end

   IF NOT (@file_count % @cpu_count = 0) and @file_count >= 8
   BEGIN
	   update #config set msg =   N'Counts do not match, but file count >=8.  CPU count ' + CONVERT(NVARCHAR(10), @cpu_count) + 
             N' FILE count = ' + CONVERT(NVARCHAR(10), @file_count)
      where category = N'TempDB data files vs physical CPUs'
   end

   IF (EXISTS(	SELECT	name,
					   size,
					   physical_name
			   FROM	tempdb.sys.database_files
			   WHERE	type = 0
			   AND		size <> (SELECT MAX(size) FROM tempdb.sys.database_files WHERE type = 0)))
      BEGIN			
         update #config set msg =   N'Tempdb data files are not equally sized. Re-run with option "@SHOW_FILE_SPACE" to see file configured and current sizes.'
         where category = N'TempDB data files are equally sized.'
      END

   DECLARE @dbccstatus table
   (
	   TraceFlag	int,
	   Status		int,
	   Global		int,
	   Session		int
   )
   INSERT INTO @dbccstatus EXEC(N'dbcc tracestatus(1118) with no_infomsgs')

   IF NOT EXISTS(SELECT * FROM	@dbccstatus WHERE TraceFlag = 1118 AND Global = 1)
   BEGIN
	   update #config set msg =    N'-T1118 is not operational'
      where category = N'Trace 1118 (no mixed extents) enabled'
   END

select '@SHOW_CONFIG_BEST_PRACTICES' as OUTPUT, * from #config


END

------------------------------------------------------------------------------------------------------------------------------------
/* OUTPUT - TEMPDB FILE UTILIZATION BY TYPE OF USE (INTERNAL, USER, VERSION STORE)*/
------------------------------------------------------------------------------------------------------------------------------------
IF @SHOW_FILE_UTILIZATION_BY_FUNCTION = 1
   BEGIN
      SELECT  '@SHOW_FILE_UTILIZATION_BY_FUNCTION'            AS OUTPUT
         , CAST(GETDATE() AS DATE)                            AS CAPTURE_DATE
         , DATEPART(HOUR, GETDATE())                          AS CAPTURE_HOUR
         , DATEPART(MINUTE, GETDATE())                        AS CAPTURE_MINUTE
         , GETDATE()                                          AS CAPTURE_DATETIME
         , CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME) AS SERVER_INSTANCE
         , CAST(SUM(USER_OBJECT_RESERVED_PAGE_COUNT)*8.0  
            /(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8  
            + SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8  
            + SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8  
            + SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8)*100 AS DECIMAL(6, 2)) AS USER_OBJECTS_PCT

         , CAST(SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8.0
            /(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8  
            + SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8  
            + SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8  
            + SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8)*100 AS DECIMAL(6, 2)) AS INTERNAL_OBJ_PCT

         , CAST(SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8.0 
            /(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8  
            + SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8  
            + SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8  
            + SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8)*100 AS DECIMAL(6, 2)) AS VERSION_STORE_PCT

         , CAST(SUM (MIXED_EXTENT_PAGE_COUNT)*8.0 
            /(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8  
            + SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8  
            + SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8  
            + SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8)*100 AS DECIMAL(6, 2)) AS MIXED_EXTENT_PCT

         , CAST(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8.0 
            /(SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8  
            + SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8  
            + SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8  
            + SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8)*100 AS DECIMAL(6, 2)) AS FREE_PCT   , SUM (USER_OBJECT_RESERVED_PAGE_COUNT)*8       AS USER_OBJECTS_KB
         , SUM (INTERNAL_OBJECT_RESERVED_PAGE_COUNT)*8   AS INTERNAL_OBJECTS_KB
         , SUM (VERSION_STORE_RESERVED_PAGE_COUNT)*8     AS VERSION_STORE_KB
         , SUM (MIXED_EXTENT_PAGE_COUNT)*8               AS MIXED_EXTENT_KB
         , SUM (UNALLOCATED_EXTENT_PAGE_COUNT)*8         AS FREE_SPACE_KB
      FROM TEMPDB.SYS.DM_DB_FILE_SPACE_USAGE
      WHERE DATABASE_ID = 2 -- TEMPDB
   END  -- IF @SHOW_FILE_UTILIZATION_BY_FUNCTION = 1



------------------------------------------------------------------------------------------------------------------------------------
/* OUTPUT - TEMPDB FILE SPACE USED AND GROWTH CONFIGURATION*/
------------------------------------------------------------------------------------------------------------------------------------
IF @SHOW_FILE_SPACE = 1
BEGIN
      IF OBJECT_ID(N'TEMPDB..#FILE_SIZE') IS NOT NULL DROP TABLE #FILE_SIZE
      IF OBJECT_ID(N'TEMPDB..#DB_FILES') IS NOT NULL DROP TABLE #DB_FILES
      IF OBJECT_ID(N'TEMPDB..##SPACE_USED') IS NOT NULL DROP TABLE ##SPACE_USED
      IF OBJECT_ID(N'TEMPDB..##FILE_GROUPS') IS NOT NULL DROP TABLE ##FILE_GROUPS
      SELECT D.DATABASE_NAME     AS DB_NAME
         , D.DATABASE_ID         AS DB_ID
         , F.DATA_SPACE_ID       AS FILE_GROUP_ID
         , F.FILE_ID             AS FILE_ID
         , F.TYPE_DESC           AS FILE_TYPE_DESC
         , F.NAME                AS FILE_NAME
         , F.PHYSICAL_NAME       AS FILE_PHYSICAL_NAME
         , CAST(D.SIZE * 8/1024.0 AS DECIMAL(18,2)) AS FILE_CONFIGURED_MB
         , CAST(F.SIZE * 8/1024.0 AS DECIMAL(18,2)) AS FILE_CURRENT_MB
         , F.MAX_SIZE              AS FILE_MAX_SIZE
         , F.GROWTH                AS FILE_GROWTH
         , F.IS_PERCENT_GROWTH     AS FILE_IS_PERCENT_GROWTH
         , ROW_NUMBER() OVER(ORDER BY D.NAME, F.TYPE_DESC DESC , F.NAME) AS ROW_NUM
         , QUOTENAME(D.NAME)     AS DB_QUOTENAME
      INTO #DB_FILES
      FROM TEMPDB.SYS.DATABASE_FILES F (NOLOCK)
       JOIN (SELECT MF.*, D.NAME AS DATABASE_NAME 
             FROM SYS.MASTER_FILES  MF
             JOIN SYS.DATABASES D ON MF.DATABASE_ID = D.DATABASE_ID 
             WHERE MF.DATABASE_ID = 2)   D ON F.FILE_ID = D.FILE_ID



      DECLARE @COUNTER INT = 1
      DECLARE @MAX_COUNTER INT
      SELECT @MAX_COUNTER = MAX(ROW_NUM) FROM #DB_FILES

      DECLARE @DB_NAME SYSNAME
      DECLARE @FILE_ID INT
      DECLARE @FILE_NAME NVARCHAR(MAX)
      DECLARE @FILE_GROUP_ID INT
      DECLARE @SQL_TEXT NVARCHAR(MAX)

      CREATE TABLE ##SPACE_USED (DB_NAME SYSNAME, FILE_ID INT, FILE_USED_MB DECIMAL(18, 2))
      CREATE TABLE ##FILE_GROUPS (DB_NAME SYSNAME, FILE_ID INT, FILE_GROUP_ID INT, FILE_GROUP_NAME SYSNAME)

      WHILE @COUNTER <= @MAX_COUNTER
      BEGIN

         SELECT @DB_NAME = DB_NAME, @FILE_ID = FILE_ID, @FILE_NAME = FILE_NAME, @FILE_GROUP_ID = FILE_GROUP_ID
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

         EXEC (@SQL_TEXT)

      SET @COUNTER = @COUNTER + 1

      END

      SELECT '@SHOW_FILE_SPACE'                               AS OUTPUT
         , CAST(GETDATE() AS DATE)                            AS CAPTURE_DATE
         , DATEPART(HOUR, GETDATE())                          AS CAPTURE_HOUR
         , DATEPART(MINUTE, GETDATE())                        AS CAPTURE_MINUTE
         , GETDATE()                                          AS CAPTURE_DATETIME
         --, CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME)      AS SERVER_INSTANCE
         --,  SERVERPROPERTY('COMPUTERNAMEPHYSICALNETBIOS')     AS SERVER
         --, SUBSTRING(FILE_PHYSICAL_NAME, 1,  CHARINDEX('\', FILE_PHYSICAL_NAME, 4)) AS VOLUME
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
         , F.FILE_CURRENT_MB - U.FILE_USED_MB AS FILE_FREE_MB
         , CASE WHEN F.FILE_CURRENT_MB = 0 THEN 0 
                ELSE CAST(U.FILE_USED_MB/F.FILE_CURRENT_MB *100 AS DECIMAL(18, 2)) END AS FILE_USED_PCT
         , CASE WHEN F.FILE_CURRENT_MB = 0 THEN 0 
                ELSE CAST((F.FILE_CURRENT_MB - U.FILE_USED_MB)/F.FILE_CURRENT_MB *100 AS DECIMAL(18, 2)) END AS FILE_FREE_PCT
      , CASE WHEN FILE_CURRENT_MB = 0 THEN 'N/A' 
             WHEN ROUND(CAST(F.FILE_CURRENT_MB - U.FILE_USED_MB AS FLOAT)/F.FILE_CURRENT_MB *100 , 2) <10 THEN 'CRITICAL - 10% OR LESS FREE'
             WHEN ROUND(CAST(F.FILE_CURRENT_MB - U.FILE_USED_MB AS FLOAT)/F.FILE_CURRENT_MB *100 , 2) <20 THEN 'WARNING - 20% OR LESS FREE' 
             ELSE 'OKAY' END AS FILE_STATUS

         , F.FILE_PHYSICAL_NAME
         , F.FILE_MAX_SIZE
         , F.FILE_GROWTH
         , F.FILE_IS_PERCENT_GROWTH
      INTO #FILE_SIZE
      FROM ##SPACE_USED U
      JOIN #DB_FILES F ON  F.FILE_ID = U.FILE_ID

      SELECT * FROM #FILE_SIZE
   END -- IF @SHOW_FILE_SPACE = 1




IF @SHOW_HISTORICAL_IO = 1 OR @SHOW_CURRENT_IO = 1

set nocount on
/* BATCH READ AND WRITE PERFORMANCE - KBs, latencies, iops
   GRAIN: PER db, per file_id, per time interval.

   Because sys.dm_io_virtual_file_stats is cumulative since restart, data have to be collected at two points in 
   time and differences calculated.

Parameters:
   |-- DATA COLLECTION TYPE:
      |--@SHOW_HISTORICAL_IO:  NO LOOPING.  Dump of sys.dm_io_virtual_file_stats.  counts as first batch.
         and/or
      |--@SHOW_CURRENT_IO:       LOOPING.  Compares data for current loop against prior loop and stores diffs.
         |--@loop_count
         |--@IO_CAPTURE_SECONDS

   |-- FILTERS
      |-- @only_show_changes.          Set to 1 to exclude values where min(calc_value) = max(calc_value).
                                       Since historical snapshot is single data set, @only_show_changes does not apply.
      |-- @only_show_nonzero.          Subset of @only_show_changes.  sometimes you want to see non-changing, non-zero values.

*/

--DECLARE @SHOW_HISTORICAL_IO    BIT         =  1 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.
--DECLARE @SHOW_CURRENT_IO         BIT         =  1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
--DECLARE @LOOP_COUNT                  INT         =  2 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
--DECLARE @IO_CAPTURE_SECONDS       INT         =  3
                                    
/* FILTER PARAMETERS*/ 
BEGIN 
      DECLARE @EXCLUDE_FILES_WITH_0_READS  BIT         =  0
      DECLARE @EXCLUDE_FILES_WITH_0_WRITES BIT         =  0

      /*SET LATENCY PERFORMANCE CRITERIA AS 'LESS THAN VALUE, IN MS'.  
      VALUES GREATER THAN THE CATEGORY 5 VALUE WILL BE FLAGGED AS 'CRITICAL'.*/
      DECLARE @LATENCY_CAT_1_EXCELLENT_MS  DECIMAL(10, 1) = 5
      DECLARE @LATENCY_CAT_2_GOOD_MS       DECIMAL(10, 1) = 10
      DECLARE @LATENCY_CAT_3_MARGINAL_MS   DECIMAL(10, 1) = 20
      DECLARE @LATENCY_CAT_4_PROBLEM_MS    DECIMAL(10, 1) = 30
      DECLARE @LATENCY_CAT_5_REAL_ISSUE_MS DECIMAL(10, 1) = 50
      ----------------------------------------------------------------------------
      /* PREP WORK*/
      DECLARE @LOOP_COUNT                            INT = 2 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB

      declare @total_time int= @loop_count * @IO_CAPTURE_SECONDS


      raiserror('|----------------------------------------------------------------', 10, 1) with nowait
      raiserror('|- Begin IO prep work', 10, 1) with nowait

      DECLARE @NOW DATETIME  = GETDATE()   --MAKE SURE ALL RECORDS IN THE BATCH ARE INSERTED WITH SAME VALUE
      DECLARE @SERVER_INSTANCE    SYSNAME = CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME)
      DECLARE @SERVER_START_TIME DATETIME = (SELECT SQLSERVER_START_TIME FROM SYS.DM_OS_SYS_INFO)
      DECLARE @DURATION    decimal(34, 3) = DATEDIFF(s, @SERVER_START_TIME, @NOW)
      DECLARE @PRIOR_DATETIME    DATETIME = NULL

      DECLARE @BATCH_COUNTER INT = 1
      DECLARE @ORIGINAL_LOOP_VALUE INT = @LOOP_COUNT

      IF OBJECT_ID(N'TEMPDB..#CURRENT') IS NOT NULL DROP TABLE #CURRENT
      CREATE TABLE #CURRENT
      (     BATCH_DATETIME                 datetime
          , DURATION_SEC                   decimal(34, 3)
          , SERVER_INSTANCE                nvarchar(128)
          , SERVER_START_DATETIME          datetime
          , DATABASE_ID                    smallint
          , DATABASE_NAME                  nvarchar(128)
          , FILE_ID                        smallint
          , FILE_NAME                      nvarchar(128)
          , FILE_TYPE                      nvarchar(60)
          , NUM_OF_READS                   bigint
          , KB_READ                        decimal(34,2)
          , AVG_KB_READ_PER_READ           decimal(34,2)
          , AVG_KB_READ_PER_SECOND         decimal(34,2)
          , AVG_READ_LATENCY_PER_READ_MS   decimal(34,2)
          , AVG_READ_LATENCY_PER_KB_MS     decimal(34,2)
          , READ_LATENCY_TOTAL_MS          bigint
          , NUM_OF_WRITES                  bigint
          , KB_WRITTEN                     decimal(34,2)
          , AVG_KB_WRITTEN_PER_READ        decimal(34,2)
          , AVG_KB_WRITTEN_PER_SECOND      decimal(34,2)
          , AVG_WRITE_LATENCY_PER_WRITE_MS decimal(34,2)
          , AVG_WRITE_LATENCY_PER_KB_MS    decimal(34,2)
          , WRITE_LATENCY_TOTAL_MS         bigint
      ) 

      IF OBJECT_ID(N'TEMPDB..#PRIOR') IS NOT NULL DROP TABLE #PRIOR
      CREATE TABLE #PRIOR
      (     PRIOR_ID                       int identity(1, 1)
          , BATCH_ID                       int
          , BATCH_DATETIME                 datetime
          , DURATION_SEC                   decimal(34, 3)
          , SERVER_INSTANCE                nvarchar(128)
          , SERVER_START_DATETIME          datetime
          , DATABASE_ID                    smallint
          , DATABASE_NAME                  nvarchar(128)
          , FILE_ID                        smallint
          , FILE_NAME                      nvarchar(128)
          , FILE_TYPE                      nvarchar(60)
          , NUM_OF_READS                   bigint
          , KB_READ                        decimal(34,2)
          , AVG_KB_READ_PER_READ           decimal(34,2)
          , AVG_KB_READ_PER_SECOND         decimal(34,2)
          , AVG_READ_LATENCY_PER_READ_MS   decimal(34,2)
          , AVG_READ_LATENCY_PER_KB_MS     decimal(34,2)
          , READ_LATENCY_TOTAL_MS          bigint
          , NUM_OF_WRITES                  bigint
          , KB_WRITTEN                     decimal(34,2)
          , AVG_KB_WRITTEN_PER_READ        decimal(34,2)
          , AVG_KB_WRITTEN_PER_SECOND      decimal(34,2)
          , AVG_WRITE_LATENCY_PER_WRITE_MS decimal(34,2)
          , AVG_WRITE_LATENCY_PER_KB_MS    decimal(34,2)
          , WRITE_LATENCY_TOTAL_MS         bigint
      ) 

      IF OBJECT_ID(N'TEMPDB..#HISTORY') IS NOT NULL DROP TABLE #HISTORY
      CREATE TABLE #HISTORY
      (     HISTORY_ID                     int identity (1, 1)
          , BATCH_ID                       int
          , BATCH_DATETIME                 datetime
          , DURATION_SEC                   decimal(34, 3)
          , SERVER_INSTANCE                nvarchar(128)
          , SERVER_START_DATETIME          datetime
          , DATABASE_ID                    smallint
          , DATABASE_NAME                  nvarchar(128)
          , FILE_ID                        smallint
          , FILE_NAME                      nvarchar(128)
          , FILE_TYPE                      nvarchar(60)
          , NUM_OF_READS                   bigint
          , KB_READ                        decimal(34,2)
          , AVG_KB_READ_PER_READ           decimal(34,2)
          , AVG_KB_READ_PER_SECOND         decimal(34,2)
          , AVG_READ_LATENCY_PER_READ_MS   decimal(34,2)
          , AVG_READ_LATENCY_PER_KB_MS     decimal(34,2)
          , READ_LATENCY_TOTAL_MS          bigint
          , NUM_OF_WRITES                  bigint
          , KB_WRITTEN                     decimal(34,2)
          , AVG_KB_WRITTEN_PER_READ        decimal(34,2)
          , AVG_KB_WRITTEN_PER_SECOND      decimal(34,2)
          , AVG_WRITE_LATENCY_PER_WRITE_MS decimal(34,2)
          , AVG_WRITE_LATENCY_PER_KB_MS    decimal(34,2)
          , WRITE_LATENCY_TOTAL_MS         bigint
      ) 

      IF OBJECT_ID(N'TEMPDB..#aggregate') IS NOT NULL DROP TABLE #aggregate
 
      CREATE TABLE #aggregate
      (
            output_type                    varchar(21)
          , batches                        int
          , DATABASE_NAME                  nvarchar(128)
          , FILE_NAME                      nvarchar(128)
          , DATABASE_ID                    smallint
          , FILE_ID                        smallint
          , FILE_TYPE                      nvarchar(60)
          , read_filter_status             varchar(19)
          , total_reads                    bigint
          , avg_reads                      bigint
          , min_reads                      bigint
          , max_reads                      bigint
          , total_KB_Read                  decimal(38,2)
          , avg_KB_Read                    decimal(38,6)
          , min_KB_Read                    decimal(34,2)
          , max_KB_Read                    decimal(34,2)
          , avg_kb_read_per_read           decimal(38,2)
          , min_kb_read_per_read           decimal(38,2)
          , max_kb_read_per_read           decimal(38,2)
          , avg_kb_read_per_second         decimal(38,2)
          , min_kb_read_per_second         decimal(38,2)
          , max_kb_read_per_second         decimal(38,2)
          , avg_read_latency_ms_per_read   decimal(38,2)
          , min_read_latency_ms_per_read   decimal(38,2)
          , max_read_latency_ms_per_read   decimal(38,2)
          , avg_read_latency_ms_per_KB     decimal(38,2)
          , min_read_latency_ms_per_KB     decimal(38,2)
          , max_read_latency_ms_per_KB     decimal(38,2)
          , write_filter_status            varchar(20)
          , total_WRITES                   bigint
          , avg_WRITES                     bigint
          , min_WRITES                     bigint
          , max_WRITES                     bigint
          , total_KB_WRITTEN               decimal(38,2)
          , avg_KB_WRITTEN                 decimal(38,6)
          , min_KB_WRITTEN                 decimal(34,2)
          , max_KB_WRITTEN                 decimal(34,2)
          , avg_KB_written_per_write       decimal(38,2)
          , min_KB_written_per_write       decimal(38,2)
          , max_KB_written_per_write       decimal(38,2)
          , avg_KB_WRITTEN_per_second      decimal(38,2)
          , min_KB_WRITTEN_per_second      decimal(38,2)
          , max_KB_WRITTEN_per_second      decimal(38,2)
          , avg_write_latency_ms_per_write decimal(38,2)
          , min_write_latency_ms_per_write decimal(38,2)
          , max_write_latency_ms_per_write decimal(38,2)
          , avg_write_latency_ms_per_KB    decimal(38,2)
          , min_write_latency_ms_per_KB    decimal(38,2)
          , max_write_latency_ms_per_KB    decimal(38,2)
      )

      IF OBJECT_ID(N'TEMPDB..#aggregate_filtered') IS NOT NULL DROP TABLE #aggregate_filtered
 
      CREATE TABLE #aggregate_filtered
      (
            output_type                    varchar(21)
          , batches                        int
          , DATABASE_NAME                  nvarchar(128)
          , FILE_NAME                      nvarchar(128)
          , DATABASE_ID                    smallint
          , FILE_ID                        smallint
          , FILE_TYPE                      nvarchar(60)
          , read_filter_status             varchar(19)
          , total_reads                    bigint
          , avg_reads                      bigint
          , min_reads                      bigint
          , max_reads                      bigint
          , total_KB_Read                  decimal(38,2)
          , avg_KB_Read                    decimal(38,6)
          , min_KB_Read                    decimal(34,2)
          , max_KB_Read                    decimal(34,2)
          , avg_kb_read_per_read           decimal(38,2)
          , min_kb_read_per_read           decimal(38,2)
          , max_kb_read_per_read           decimal(38,2)
          , avg_kb_read_per_second         decimal(38,2)
          , min_kb_read_per_second         decimal(38,2)
          , max_kb_read_per_second         decimal(38,2)
          , avg_read_latency_ms_per_read   decimal(38,2)
          , min_read_latency_ms_per_read   decimal(38,2)
          , max_read_latency_ms_per_read   decimal(38,2)
          , avg_read_latency_ms_per_KB     decimal(38,2)
          , min_read_latency_ms_per_KB     decimal(38,2)
          , max_read_latency_ms_per_KB     decimal(38,2)
          , write_filter_status            varchar(20)
          , total_WRITES                   bigint
          , avg_WRITES                     bigint
          , min_WRITES                     bigint
          , max_WRITES                     bigint
          , total_KB_WRITTEN               decimal(38,2)
          , avg_KB_WRITTEN                 decimal(38,6)
          , min_KB_WRITTEN                 decimal(34,2)
          , max_KB_WRITTEN                 decimal(34,2)
          , avg_KB_written_per_write       decimal(38,2)
          , min_KB_written_per_write       decimal(38,2)
          , max_KB_written_per_write       decimal(38,2)
          , avg_KB_WRITTEN_per_second      decimal(38,2)
          , min_KB_WRITTEN_per_second      decimal(38,2)
          , max_KB_WRITTEN_per_second      decimal(38,2)
          , avg_write_latency_ms_per_write decimal(38,2)
          , min_write_latency_ms_per_write decimal(38,2)
          , max_write_latency_ms_per_write decimal(38,2)
          , avg_write_latency_ms_per_KB    decimal(38,2)
          , min_write_latency_ms_per_KB    decimal(38,2)
          , max_write_latency_ms_per_KB    decimal(38,2)
      )

      Raiserror('|- End   prep work', 10, 1) with nowait
      Raiserror('|----------------------------------------------------------------', 10, 1) with nowait

      raiserror('|----------------------------------------------------------------', 10, 1) with nowait
      raiserror('|- Begin loop', 10, 1) with nowait
      WHILE @LOOP_COUNT >= 1
         BEGIN
            print @loop_count

            SET @NOW = GETDATE()

            raiserror('   |--- Begin Insert into #CURRENT ', 10, 1) with nowait

            TRUNCATE TABLE #CURRENT

            INSERT INTO #CURRENT
               SELECT  
                    @NOW                                      AS BATCH_DATETIME
                  , @DURATION                                 AS DURATION_SEC 
                  , @SERVER_INSTANCE                          AS SERVER_INSTANCE 
                  , @SERVER_START_TIME                        AS SERVER_START_DATETIME
                  , IO.DATABASE_ID                            AS DATABASE_ID
                  , D.NAME                                    AS DATABASE_NAME
                  , IO.FILE_ID                                AS FILE_ID
                  , MF.NAME                                   AS FILE_NAME
                  , CASE WHEN MF.TYPE_DESC  = 'ROWS'          
                         THEN 'DATA'                          
                         ELSE MF.type_desc END                AS FILE_TYPE
                  , NUM_OF_READS                                                                     AS NUM_OF_READS
                  , cast(NUM_OF_BYTES_READ/1024.0 as decimal(34, 2))                                 AS KB_READ
                  , CAST(CASE WHEN NUM_OF_READS = 0 
                              THEN 0 
                              ELSE NUM_OF_BYTES_READ/1024.0/NUM_OF_READS END AS decimal(34,2))       AS AVG_KB_READ_PER_READ
                  , CAST(CASE WHEN @DURATION = 0 
                              THEN 0 
                              ELSE NUM_OF_BYTES_READ/1024.0/@DURATION END AS decimal(34,2))          AS AVG_KB_READ_PER_SECOND
                  , CAST(CASE WHEN NUM_OF_READS = 0 
                              THEN 0 
                              ELSE IO_STALL_READ_MS/1.0/NUM_OF_READS END AS decimal(34,2))           AS AVG_READ_LATENCY_PER_READ_MS
                  , CAST(CASE WHEN NUM_OF_BYTES_READ= 0 
                              THEN 0 
                              ELSE IO_STALL_READ_MS/(NUM_OF_BYTES_READ/1024.0) END AS decimal(34,2)) AS AVG_READ_LATENCY_PER_KB_MS
                  , IO_STALL_READ_MS                                                                 AS READ_LATENCY_TOTAL_MS
                  , NUM_OF_WRITES                                                                    AS NUM_OF_WRITES
                  , cast(NUM_OF_BYTES_WRITTEN/1024.0 as decimal(34, 2))                              AS KB_WRITTEN
                  , CAST(CASE WHEN NUM_OF_WRITES = 0 
                              THEN 0 
                              ELSE NUM_OF_BYTES_WRITTEN/1024.0/NUM_OF_WRITES END AS decimal(34,2))   AS AVG_KB_WRITTEN_PER_READ
                  , CAST(CASE WHEN @DURATION = 0 
                              THEN 0 
                              ELSE NUM_OF_BYTES_WRITTEN/1024.0/@DURATION END AS decimal(34,2))       AS AVG_KB_WRITTEN_PER_SECOND
                  , CAST(CASE WHEN NUM_OF_WRITES = 0 
                              THEN 0 
                              ELSE IO_STALL_WRITE_MS/NUM_OF_WRITES END AS decimal(34,2))             AS AVG_WRITE_LATENCY_PER_READ_MS
                  , CAST(CASE WHEN NUM_OF_WRITES = 0 
                              THEN 0 
                              ELSE IO_STALL_WRITE_MS/(NUM_OF_BYTES_WRITTEN/1024.0) END AS decimal(34,2)) AS AVG_WRITE_LATENCY_PER_KB_MS
                  , IO_STALL_WRITE_MS                                                                    AS WRITE_LATENCY_TOTAL_MS
               FROM SYS.DM_IO_VIRTUAL_FILE_STATS(2,NULL) IO
                  JOIN sys.databases                         D ON D.database_id = IO.database_id
                  JOIN SYS.MASTER_FILES                     MF ON MF.FILE_ID = IO.FILE_ID
                                                                  AND MF.DATABASE_ID = IO.DATABASE_ID

            raiserror('   |--- End   Insert into #CURRENT ', 10, 1) with nowait

            IF @SHOW_HISTORICAL_IO = 1 and @BATCH_COUNTER = 1
               BEGIN
                  SELECT 'IO - History' as OUTPUT_TYPE
                  , @BATCH_COUNTER AS BATCH_ID
                   , BATCH_DATETIME
                   , DURATION_SEC
                   , SERVER_INSTANCE
                   , SERVER_START_DATETIME
                   , DATABASE_ID
                   , DATABASE_NAME
                   , FILE_ID
                   , FILE_NAME
                   , FILE_TYPE
                  , CASE WHEN NUM_OF_READS = 0 then 'n/a - 0 reads'                  
                         else CASE WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)   * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                                   WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)   * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                                   WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)   * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                                   WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)   * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                                   WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)   * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
                                   ELSE '5- CRITICAL'                               
                              END                                                   
                     END AS READ_STALL_CATEGORY                                     
                  , CASE WHEN NUM_OF_WRITES = 0 then 'n/a - 0 writes'               
                         else CASE WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                                   WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                                   WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                                   WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                                   WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
                                   ELSE '5- CRITICAL'
                              END
                     END AS WRITE_STALL_CATEGORY
                   , NUM_OF_READS
                   , KB_READ
                   , AVG_KB_READ_PER_READ
                   , AVG_KB_READ_PER_SECOND
                   , AVG_READ_LATENCY_PER_READ_MS
                   , AVG_READ_LATENCY_PER_KB_MS
                   , READ_LATENCY_TOTAL_MS
                   , NUM_OF_WRITES
                   , KB_WRITTEN
                   , AVG_KB_WRITTEN_PER_READ
                   , AVG_KB_WRITTEN_PER_SECOND
                   , AVG_WRITE_LATENCY_PER_WRITE_MS
                   , AVG_WRITE_LATENCY_PER_KB_MS
                   , WRITE_LATENCY_TOTAL_MS
                  FROM #CURRENT
                  WHERE ((@EXCLUDE_FILES_WITH_0_READS = 1 AND NUM_OF_READS > 0 OR @EXCLUDE_FILES_WITH_0_READS = 0)
                        or 
                        (@EXCLUDE_FILES_WITH_0_WRITES = 1 AND NUM_OF_WRITES > 0 OR @EXCLUDE_FILES_WITH_0_WRITES = 0))
                  ORDER BY DATABASE_NAME, FILE_TYPE, FILE_NAME

              raiserror('   |--------- Begin IF @SHOW_CURRENT_IO = 0  ', 10, 1) with nowait

               --IF @SHOW_CURRENT_IO = 0 
               --   BEGIN
               --      RETURN
               --   END --IF @SHOW_CURRENT_IO = 0 

              raiserror('   |--------- End   IF @SHOW_CURRENT_IO = 0   ', 10, 1) with nowait

               END --IF @SHOW_HISTORICAL_IO = 1

              ----------------------------------------------------------------------------
               /* A.  if first collection, 
                         i.  load staging table 
                         ii.  load history table.
               */

            raiserror('   |--- Begin IF @ORIGINAL_LOOP_VALUE = @LOOP_COUNT  ', 10, 1) with nowait

            IF @ORIGINAL_LOOP_VALUE = @LOOP_COUNT 
               BEGIN
                  INSERT INTO #PRIOR
                     SELECT @BATCH_COUNTER, * FROM #CURRENT
       
                  INSERT INTO #HISTORY
                     SELECT @BATCH_COUNTER, * FROM #CURRENT

                  SET @PRIOR_DATETIME = @NOW
                  SET @NOW = GETDATE()

                  --select 'a', @ORIGINAL_LOOP_VALUE, @LOOP_COUNT,  @BATCH_COUNTER,  convert(varchar(100), @now, 120), convert(varchar(100), @prior_datetime, 120)
            raiserror('   |--- End   IF @ORIGINAL_LOOP_VALUE = @LOOP_COUNT ', 10, 1) with nowait

               END --IF @ORIGINAL_LOOP_VALUE = @LOOP_COUNT 


               ----------------------------------------------------------------------------
               /*
                   B.  if subsequent collection
                         i.  compare values from current collection and staging (Prior) collection.
                         ii.  load calculated values into history
                         iii.  load current collection into staging
               */
     
            else IF @ORIGINAL_LOOP_VALUE <> @LOOP_COUNT 
               BEGIN
             raiserror('   |--- Begin IF @ORIGINAL_LOOP_VALUE <> @LOOP_COUNT  ', 10, 1) with nowait
                 --select 'b', @ORIGINAL_LOOP_VALUE, @LOOP_COUNT,  @BATCH_COUNTER,  convert(varchar(100), @now, 120), convert(varchar(100), @prior_datetime, 120)
                  --set @NOW = GETDATE()
                  set @DURATION = DATEDIFF(ms, @PRIOR_DATETIME, @NOW)/1000.0

                  INSERT INTO #HISTORY
                     SELECT @BATCH_COUNTER
                     , C.BATCH_DATETIME
                     , @DURATION              
                     , C.SERVER_INSTANCE
                     , C.SERVER_START_DATETIME
                     , C.DATABASE_ID
                     , C.DATABASE_NAME
                     , C.FILE_ID
                     , C.FILE_NAME
                     , C.FILE_TYPE
                     , coalesce(C.NUM_OF_READS, 0) - coalesce(P.NUM_OF_READS , 0) AS NUM_OF_READS
                     , coalesce(C.KB_READ , 0)     - coalesce(P.KB_READ , 0)      AS KB_READ

                     , CAST(CASE WHEN COALESCE(C.NUM_OF_READS, 0) - COALESCE(P.NUM_OF_READS , 0) = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.KB_READ , 0)- COALESCE(P.KB_READ , 0)) * 1.0 
                                       /(COALESCE(C.NUM_OF_READS, 0) - COALESCE(P.NUM_OF_READS , 0)) end AS decimal(34,2)) as AVG_KB_READ_PER_READ

                     , CAST(CASE WHEN @DURATION = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.KB_READ , 0)- COALESCE(P.KB_READ , 0)) * 1.0 /@DURATION end AS decimal(34,2)) as AVG_KB_READ_PER_SECOND
               
                     , CAST(CASE WHEN COALESCE(C.NUM_OF_READS, 0) - COALESCE(P.NUM_OF_READS , 0) = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.READ_LATENCY_TOTAL_MS, 0) - COALESCE( P.READ_LATENCY_TOTAL_MS, 0)) * 1.0 
                                      /(COALESCE(C.NUM_OF_READS, 0) - COALESCE(P.NUM_OF_READS , 0))  END AS decimal(34,2)) AS AVG_READ_LATENCY_PER_READ_MS

                     , CAST(CASE WHEN coalesce(C.KB_READ , 0)- coalesce(P.KB_READ , 0) = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.READ_LATENCY_TOTAL_MS, 0) - COALESCE( P.READ_LATENCY_TOTAL_MS, 0)) * 1.0 
                                      /(coalesce(C.KB_READ , 0)- coalesce(P.KB_READ , 0))  END AS decimal(34,2)) AS AVG_READ_LATENCY_PER_KB_MS

                     , COALESCE(C.READ_LATENCY_TOTAL_MS, 0) -COALESCE( P.READ_LATENCY_TOTAL_MS, 0) AS READ_LATENCY_TOTAL_MS
               
                     , COALESCE(C.NUM_OF_WRITES , 0) - COALESCE(P.NUM_OF_WRITES, 0) AS NUM_OF_WRITES
               
                     , COALESCE(C.KB_WRITTEN, 0)     - COALESCE(P.KB_WRITTEN, 0)    AS KB_WRITTEN
               
                     , CAST(CASE WHEN COALESCE(C.NUM_OF_WRITES , 0)- COALESCE(P.NUM_OF_WRITES, 0) = 0 
                                THEN 0 
                                ELSE (COALESCE(C.KB_WRITTEN, 0) - COALESCE(P.KB_WRITTEN, 0)) * 1.0 
                                     /(COALESCE(C.NUM_OF_WRITES , 0)- COALESCE(P.NUM_OF_WRITES, 0)) END AS decimal(34,2)) AS AVG_KB_WRITTEN_PER_READ

                     , CAST(CASE WHEN @DURATION = 0 
                                THEN 0 
                                ELSE (COALESCE(C.KB_WRITTEN, 0) - COALESCE(P.KB_WRITTEN, 0)) * 1.0 / @DURATION END AS decimal(34,2)) AS AVG_KB_WRITTEN_PER_SECOND
               
                     , CAST(CASE WHEN COALESCE(C.NUM_OF_WRITES , 0)- COALESCE(P.NUM_OF_WRITES, 0) = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.WRITE_LATENCY_TOTAL_MS, 0) - COALESCE(P.WRITE_LATENCY_TOTAL_MS, 0)) * 1.0 
                                       /(COALESCE(C.NUM_OF_WRITES , 0)- COALESCE(P.NUM_OF_WRITES, 0)) END AS decimal(34,2)) AS AVG_WRITE_LATENCY_PER_WRITE_MS

                     , CAST(CASE WHEN COALESCE(C.KB_WRITTEN, 0) - COALESCE(P.KB_WRITTEN, 0) = 0 
                                 THEN 0 
                                 ELSE (COALESCE(C.WRITE_LATENCY_TOTAL_MS, 0) - COALESCE(P.WRITE_LATENCY_TOTAL_MS, 0)) * 1.0 
                                       /(COALESCE(C.KB_WRITTEN, 0) - COALESCE(P.KB_WRITTEN, 0)) END AS decimal(34,2)) AS AVG_WRITE_LATENCY_PER_KB_MS

                     , COALESCE(C.WRITE_LATENCY_TOTAL_MS, 0) - COALESCE(P.WRITE_LATENCY_TOTAL_MS, 0)
                     FROM #CURRENT    C
                     FULL JOIN #PRIOR P ON P.SERVER_INSTANCE = C.SERVER_INSTANCE
                                           AND P.DATABASE_ID = C.DATABASE_ID
                                           AND P.FILE_ID = C.FILE_ID

                  truncate table #prior

                  INSERT INTO #PRIOR
                     SELECT @BATCH_COUNTER, * FROM #CURRENT
       



               END

            raiserror('   |--- End   IF @ORIGINAL_LOOP_VALUE <> @LOOP_COUNT ', 10, 1) with nowait

            SET @LOOP_COUNT = @LOOP_COUNT - 1
            SET @BATCH_COUNTER = @BATCH_COUNTER + 1
            SET  @PRIOR_DATETIME = @NOW 

            waitfor delay @IO_CAPTURE_SECONDS

         END --WHILE @COUNTER <= @LOOP_COUNT
      Raiserror('|- End   loop', 10, 1) with nowait
      Raiserror('|----------------------------------------------------------------', 10, 1) with nowait

      ----------------------------------------------------------------------------------------------------

      raiserror('|----------------------------------------------------------------', 10, 1) with nowait
      raiserror('|- Begin output from loop', 10, 1) with nowait

      /* get aggregates - used for reporting and for filtering details*/
      insert into #aggregate
         select 'IO - Loops Aggregated' as output_type
         , count(*)                 as batches
         , DATABASE_NAME
         , FILE_NAME 
         , DATABASE_ID
         , FILE_ID
         , FILE_TYPE
         , case when sum(NUM_OF_READS) = 0 then 'No Reads'
                when min(NUM_OF_READS) = max(NUM_OF_READS) then 'No Changes in Reads' else '' end as read_filter_status

         , sum(NUM_OF_READS) as total_reads
         , avg(NUM_OF_READS) as avg_reads
         , min(NUM_OF_READS) as min_reads
         , max(NUM_OF_READS) as max_reads

         , sum(KB_READ) as total_KB_Read
         , avg(KB_READ) as avg_KB_Read
         , min(KB_READ) as min_KB_Read
         , max(KB_READ) as max_KB_Read

          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else sum(KB_READ) * 1.0/sum(NUM_OF_READS)               end AS DECIMAL(38, 2)) as avg_kb_read_per_read
          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else min(KB_READ) * 1.0/sum(NUM_OF_READS)               end AS DECIMAL(38, 2)) as min_kb_read_per_read
          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else max(KB_READ) * 1.0/sum(NUM_OF_READS)               end AS DECIMAL(38, 2)) as max_kb_read_per_read
                                                                                              
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else sum(KB_READ)/sum(DURATION_SEC)                     end AS DECIMAL(38, 2)) as avg_kb_read_per_second
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else min(KB_READ)/sum(DURATION_SEC)                     end AS DECIMAL(38, 2)) as min_kb_read_per_second
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else max(KB_READ)/sum(DURATION_SEC)                     end AS DECIMAL(38, 2)) as max_kb_read_per_second
                                                                                              
          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else sum(READ_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_READS) end AS DECIMAL(38, 2)) as avg_read_latency_ms_per_read
          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else min(READ_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_READS) end AS DECIMAL(38, 2)) as min_read_latency_ms_per_read
          , CAST(case when sum(NUM_OF_READS) = 0 then 0 else max(READ_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_READS) end AS DECIMAL(38, 2)) as max_read_latency_ms_per_read
                                                                                               
          , CAST(case when sum(KB_READ) = 0 then 0 else sum(READ_LATENCY_TOTAL_MS) * 1.0/sum(KB_READ)           end AS DECIMAL(38, 2)) as avg_read_latency_ms_per_KB
          , CAST(case when sum(KB_READ) = 0 then 0 else min(READ_LATENCY_TOTAL_MS) * 1.0/sum(KB_READ)           end AS DECIMAL(38, 2)) as min_read_latency_ms_per_KB
          , CAST(case when sum(KB_READ) = 0 then 0 else max(READ_LATENCY_TOTAL_MS) * 1.0/sum(KB_READ)           end AS DECIMAL(38, 2)) as max_read_latency_ms_per_KB

      -------------------------------------------------------------------------------------------------------------------------------------
         , case when sum(NUM_OF_WRITES) = 0 then 'No Writes'
                when min(NUM_OF_WRITES) = max(NUM_OF_WRITES) then 'No Changes in Writes' else '' end as write_filter_status

         , sum(NUM_OF_WRITES) as total_WRITES
         , avg(NUM_OF_WRITES) as avg_WRITES
         , min(NUM_OF_WRITES) as min_WRITES
         , max(NUM_OF_WRITES) as max_WRITES

         , sum(KB_WRITTEN) as total_KB_WRITTEN
         , avg(KB_WRITTEN) as avg_KB_WRITTEN
         , min(KB_WRITTEN) as min_KB_WRITTEN
         , max(KB_WRITTEN) as max_KB_WRITTEN

          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else sum(KB_WRITTEN) * 1.0/sum(NUM_OF_WRITES)            end AS DECIMAL(38, 2)) as avg_KB_written_per_write
          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else min(KB_WRITTEN) * 1.0/sum(NUM_OF_WRITES)            end AS DECIMAL(38, 2)) as min_KB_written_per_write
          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else max(KB_WRITTEN) * 1.0/sum(NUM_OF_WRITES)            end AS DECIMAL(38, 2)) as max_KB_written_per_write
                                                                                                                                          
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else sum(KB_WRITTEN)/sum(DURATION_SEC)                    end AS DECIMAL(38, 2)) as avg_KB_WRITTEN_per_second
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else min(KB_WRITTEN)/sum(DURATION_SEC)                    end AS DECIMAL(38, 2)) as min_KB_WRITTEN_per_second
          , CAST(case when sum(DURATION_SEC) = 0 then 0 else max(KB_WRITTEN)/sum(DURATION_SEC)                    end AS DECIMAL(38, 2)) as max_KB_WRITTEN_per_second
                                                                                                               
          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else sum(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_WRITES)end AS DECIMAL(38, 2)) as avg_write_latency_ms_per_write
          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else min(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_WRITES)end AS DECIMAL(38, 2)) as min_write_latency_ms_per_write
          , CAST(case when sum(NUM_OF_WRITES) = 0 then 0 else max(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(NUM_OF_WRITES)end AS DECIMAL(38, 2)) as max_write_latency_ms_per_write
                                                                                                               
          , CAST(case when sum(KB_WRITTEN) = 0 then 0 else sum(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(KB_WRITTEN)      end AS DECIMAL(38, 2)) as avg_write_latency_ms_per_KB
          , CAST(case when sum(KB_WRITTEN) = 0 then 0 else min(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(KB_WRITTEN)      end AS DECIMAL(38, 2)) as min_write_latency_ms_per_KB
          , CAST(case when sum(KB_WRITTEN) = 0 then 0 else max(WRITE_LATENCY_TOTAL_MS) * 1.0/sum(KB_WRITTEN)      end AS DECIMAL(38, 2)) as max_write_latency_ms_per_KB

         from #HISTORY
         where batch_id <> 1
         group by
          DATABASE_NAME
         , FILE_NAME 
         , DATABASE_ID
         , FILE_ID
         , FILE_TYPE

      insert into #aggregate_filtered
         select * 
         from #aggregate  a
                  WHERE ((@EXCLUDE_FILES_WITH_0_READS = 1 AND total_reads > 0 OR @EXCLUDE_FILES_WITH_0_READS = 0)
                        or 
                        (@EXCLUDE_FILES_WITH_0_WRITES = 1 AND total_WRITES > 0 OR @EXCLUDE_FILES_WITH_0_WRITES = 0))

      IF OBJECT_ID(N'TEMPDB..#history_categories') IS NOT NULL DROP TABLE #history_categories
 
      SELECT  h.batch_id
            , h.DATABASE_ID
            , h.FILE_ID
            , CASE WHEN NUM_OF_READS = 0 then 'n/a - 0 reads' 
                   else CASE WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0) * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0) * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0) * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0) * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0) * 1.0 / NUM_OF_READS   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
                             ELSE '5- CRITICAL'
                        END
               END AS READ_STALL_CATEGORY
            , CASE WHEN NUM_OF_WRITES = 0 then 'n/a - 0 writes' 
                   else CASE WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0) * 1.0 / NUM_OF_WRITES   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
                             ELSE '5- CRITICAL'
                        END
               END AS WRITE_STALL_CATEGORY
      into #history_categories
      from #history h
      join #aggregate_filtered af on h.DATABASE_ID = af.DATABASE_ID
                                 and h.FILE_ID = af.FILE_ID
      where batch_id > 1

      IF OBJECT_ID(N'TEMPDB..#READ_CATEGORIES') IS NOT NULL DROP TABLE #READ_CATEGORIES
      IF OBJECT_ID(N'TEMPDB..#WRITE_CATEGORIES') IS NOT NULL DROP TABLE #WRITE_CATEGORIES
      IF OBJECT_ID(N'TEMPDB..#read_category_pivot') IS NOT NULL DROP TABLE #read_category_pivot
      IF OBJECT_ID(N'TEMPDB..#write_category_pivot') IS NOT NULL DROP TABLE #write_category_pivot
 
      SELECT DATABASE_ID, FILE_ID, READ_STALL_CATEGORY, COUNT(*) AS BATCHES
      INTO #READ_CATEGORIES
      FROM #history_categories
      GROUP BY DATABASE_ID, FILE_ID, READ_STALL_CATEGORY

      SELECT DATABASE_ID, FILE_ID, WRITE_STALL_CATEGORY, COUNT(*) AS BATCHES
      INTO #WRITE_CATEGORIES
      FROM #history_categories
      GROUP BY DATABASE_ID, FILE_ID, WRITE_STALL_CATEGORY

      select 
            DATABASE_ID, FILE_ID,  [0- EXCELLENT], [1- GOOD], [2- MARGINAL], [3- PROBLEM], [4- REAL PROBLEM] , [5- CRITICAL]
      , (coalesce([0- EXCELLENT], 0) + coalesce([1- GOOD], 0) +  coalesce([2- MARGINAL], 0) + coalesce([3- PROBLEM], 0)  + coalesce([4- REAL PROBLEM], 0) + coalesce([5- CRITICAL], 0)) as filtered_batches
      into #read_category_pivot
         from #READ_CATEGORIES h
      pivot (sum(BATCHES) FOR READ_STALL_CATEGORY IN ([0- EXCELLENT], [1- GOOD], [2- MARGINAL], [3- PROBLEM], [4- REAL PROBLEM] , [5- CRITICAL] )) as p   ;
                                                 
      select 
            DATABASE_ID, FILE_ID,  [0- EXCELLENT], [1- GOOD], [2- MARGINAL], [3- PROBLEM], [4- REAL PROBLEM] , [5- CRITICAL]
      , (coalesce([0- EXCELLENT], 0) + coalesce([1- GOOD], 0) +  coalesce([2- MARGINAL], 0) + coalesce([3- PROBLEM], 0)  + coalesce([4- REAL PROBLEM], 0) + coalesce([5- CRITICAL], 0)) as filtered_batches
      into #write_category_pivot
         from #write_CATEGORIES h
      pivot (sum(BATCHES) FOR write_STALL_CATEGORY IN ([0- EXCELLENT], [1- GOOD], [2- MARGINAL], [3- PROBLEM], [4- REAL PROBLEM] , [5- CRITICAL] )) as p   ;

      SELECT
            af.output_type
          , af.DATABASE_NAME
          , af.FILE_NAME
          , af.DATABASE_ID
          , af.FILE_ID
          , af.FILE_TYPE
          , af.batches
          , rcp.filtered_batches as batches_with_reads
          , wcp.filtered_batches as batches_with_writes
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[0- EXCELLENT]    * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as excellent_read_batches_pct 
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[1- GOOD]         * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as good_read_batches_pct     
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[2- MARGINAL]     * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as marginal_read_batches_pct
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[3- PROBLEM]      * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as problem_read_batches_pct
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[4- REAL PROBLEM] * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as real_problem_read_batches_pct
          , cast(case when rcp.filtered_batches = 0 then 0 else rcp.[5- CRITICAL]     * 100.0 / rcp.filtered_batches  end as decimal(38, 2)) as critical_read_batches_pct
                                                                               
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[0- EXCELLENT]    * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as excellent_write_batches_pct 
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[1- GOOD]         * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as good_write_batches_pct     
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[2- MARGINAL]     * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as marginal_write_batches_pct
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[3- PROBLEM]      * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as problem_write_batches_pct
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[4- REAL PROBLEM] * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as real_problem_write_batches_pct
          , cast(case when wcp.filtered_batches = 0 then 0 else wcp.[5- CRITICAL]     * 100.0 / wcp.filtered_batches end as decimal(38, 2)) as critical_write_batches_pct

          , af.read_filter_status
          , af.total_reads
          , af.avg_reads
          , af.min_reads
          , af.max_reads     
          , af.total_KB_Read
          , af.avg_KB_Read
          , af.min_KB_Read
          , af.max_KB_Read
          , af.avg_kb_read_per_read
          , af.min_kb_read_per_read
          , af.max_kb_read_per_read
          , af.avg_kb_read_per_second
          , af.min_kb_read_per_second
          , af.max_kb_read_per_second
          , af.avg_read_latency_ms_per_read
          , af.min_read_latency_ms_per_read
          , af.max_read_latency_ms_per_read
          , af.avg_read_latency_ms_per_KB
          , af.min_read_latency_ms_per_KB
          , af.max_read_latency_ms_per_KB
          , af.write_filter_status
          , af.total_WRITES
          , af.avg_WRITES
          , af.min_WRITES
          , af.max_WRITES
          , af.total_KB_WRITTEN
          , af.avg_KB_WRITTEN
          , af.min_KB_WRITTEN
          , af.max_KB_WRITTEN
          , af.avg_KB_written_per_write
          , af.min_KB_written_per_write
          , af.max_KB_written_per_write
          , af.avg_KB_WRITTEN_per_second
          , af.min_KB_WRITTEN_per_second
          , af.max_KB_WRITTEN_per_second
          , af.avg_write_latency_ms_per_write
          , af.min_write_latency_ms_per_write
          , af.max_write_latency_ms_per_write
          , af.avg_write_latency_ms_per_KB
          , af.min_write_latency_ms_per_KB
          , af.max_write_latency_ms_per_KB
      FROM #aggregate_filtered af
      join #read_category_pivot rcp on rcp.DATABASE_ID = af.DATABASE_ID
                                      and rcp.FILE_ID = af.FILE_ID
      join #write_category_pivot wcp on wcp.DATABASE_ID = af.DATABASE_ID
                                      and wcp.FILE_ID = af.FILE_ID
                                                      

      Raiserror('|- End   output from loop', 10, 1) with nowait
      Raiserror('|----------------------------------------------------------------', 10, 1) with nowait
END -- IF @HISTORICAL_IO = 1 OR @CURRENT_IO = 1

------------------------------------------------------------------------------------------------------------
/* OUTPUT - PERFMON COUNTERS*/
----------------------------------------------------------------------------------------------------------------------------------
IF @SHOW_PERFMON_COUNTERS = 1
   BEGIN
      IF OBJECT_ID(N'TEMPDB..#PERFMON_1') is not null DROP TABLE #PERFMON_1
      IF OBJECT_ID(N'TEMPDB..#PERFMON_2') is not null DROP TABLE #PERFMON_2


      SELECT *
      INTO #PERFMON_1
      FROM SYS.DM_OS_PERFORMANCE_COUNTERS
      WHERE counter_name like '%Temp Tables%'
      Or  instance_name = 'tempdb'
      Or (counter_name like '%version%'
         and object_name like '%transactions%')
      or counter_name in ('Workfiles Created/sec', 'Worktables Created/sec')  

      WAITFOR DELAY @PERFMON_COUNTER_COLLECTION_TIME_SECS

      SELECT *
      INTO #PERFMON_2
      FROM SYS.DM_OS_PERFORMANCE_COUNTERS
      WHERE counter_name like '%Temp Tables%'
      Or  instance_name = 'tempdb'
      Or (counter_name like '%version%'
         and object_name like '%transactions%')
      or counter_name in ('Workfiles Created/sec', 'Worktables Created/sec')                                                                                                          


      SELECT '@SHOW_PERFMON_COUNTERS' AS OUTPUT
      , object_name
      , counter_name
      , instance_name
      , cntr_value 
      , cast('flat count - 65792' as nvarchar(50)) as counter_type
      FROM #PERFMON_2
      WHERE CNTR_TYPE = 65792

      UNION ALL 

      SELECT '@SHOW_PERFMON_COUNTERS' AS OUTPUT
      , p2.object_name
      , p2.counter_name
      , p2.instance_name
      ,cast((coalesce(P2.CNTR_VALUE, 0) - coalesce(P1.cntr_value, 0)) * 1.0/@PERFMON_COUNTER_COLLECTION_TIME_SECS as decimal(12, 2)) as  cntr_value
      , cast('(t1 - t2) / seconds - 272696576' as nvarchar(50)) as counter_type
      from #perfmon_2 p2
      left join #perfmon_1 p1 on p1.object_name = p2.object_name
                              and p1.counter_name = p2.counter_name
                              and p1.instance_name = p2.instance_name
      where p2.cntr_type = 272696576

      union all

      SELECT '@SHOW_PERFMON_COUNTERS' AS OUTPUT
      , p2.object_name
      , p2.counter_name
      , p2.instance_name
      , p2.cntr_value
      , cast(p2.cntr_type as nvarchar(50))
      from #perfmon_2 p2
      left join #perfmon_1 p1 on p1.object_name = p2.object_name
                              and p1.counter_name = p2.counter_name
                              and p1.instance_name = p2.instance_name
      where p2.cntr_type not in ( 272696576, 65792)
      order by object_name, counter_name

   END  -- IF @SHOW_PERFMON_COUNTERS = 1
----------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------
/* OUTPUT - OLDEST OPEN TRANSACTION*/
------------------------------------------------------------------------------------------------------------
IF @SHOW_OLDEST_TRANSACTION = 1
   BEGIN
      IF OBJECT_ID(N'TEMPDB..#OPENTRANSTATUS') IS NOT NULL DROP TABLE #OPENTRANSTATUS

      CREATE TABLE #OPENTRANSTATUS (
         ACTIVETRANSACTION VARCHAR(25),
         DETAILS SQL_VARIANT 
         )
      -- EXECUTE THE COMMAND, PUTTING THE RESULTS IN THE TABLE.
      INSERT INTO #OPENTRANSTATUS 
         EXEC ('DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS');

      SET @NOW  = GETDATE()

      DECLARE @SPID INT, @STARTTIME DATETIME,  @DURATION_SECONDS INT

      SELECT @SPID = CAST(DETAILS AS INT) FROM #OPENTRANSTATUS WHERE ACTIVETRANSACTION = 'OLDACT_SPID'
      SELECT @STARTTIME = CAST(DETAILS AS DATETIME) FROM #OPENTRANSTATUS WHERE ACTIVETRANSACTION = 'OLDACT_STARTTIME'
      SELECT @DURATION_SECONDS = DATEDIFF(SECOND, @STARTTIME, @NOW)

      SELECT '@SHOW_OLDEST_TRANSACTION' AS OUTPUT, @SPID AS SPID, @STARTTIME AS START_TIME, @DURATION_SECONDS AS DURATION_SECS, CAST(@DURATION_SECONDS/60.0 AS DECIMAL(10, 2)) AS DURATION_MINUTES
   END  -- IF @SHOW_OLDEST_TRANSACTION = 1

------------------------------------------------------------------------------------------------------------
/* OUTPUT - LOG SIZE*/
------------------------------------------------------------------------------------------------------------
IF @SHOW_LOG_INFO = 1
   BEGIN
      SELECT '@SHOW_LOG_INFO' AS OUTPUT, @@SERVERNAME, LOG_REUSE_WAIT_DESC AS TEMPDB_LOG_REUSE_WAIT
      , (SELECT CNTR_VALUE FROM SYS.DM_OS_PERFORMANCE_COUNTERS C WHERE C.COUNTER_NAME= 'PERCENT LOG USED' AND INSTANCE_NAME = 'TEMPDB') AS PCT_LOG_USED
      , 'TEMPDB LOG FLUSHES AUTOMATICALLY AT 70%.  IF >70%, LOOK AT ACTIVE TRANSACTIONS.'
      FROM SYS.DATABASES
      WHERE DATABASE_ID = 2
   END --IF @SHOW_LOG_INFO = 1

------------------------------------------------------------------------------------------------------------------------------------
/* PAGE ALLOCATION CONTENTION OUTPUT*/
------------------------------------------------------------------------------------------------------------------------------------

 IF @SHOW_PAGE_ALLOCATION_CONTENTION = 1
   BEGIN
      declare @page_allocation_duration_sec int =  @PAGE_ALLOCATION_CONTENTION_LOOPS * @PAGE_ALLOCATION_CONTENTION_SECS_PER
      declare @page_allocation_duration_min decimal(12, 2) = @page_allocation_duration_sec/60.0
      declare @page_allocation_completion nvarchar(100) =cast( dateadd(s, @page_allocation_duration_sec, getdate()) as nvarchar(100))

      raiserror('|- Page allocation contention loop will finish in %d seconds. ', 10, 1, @page_allocation_duration_sec) with nowait
      IF OBJECT_ID(N'TEMPDB..#PAGE_ALLOCATION_CONTENTION') IS NOT NULL DROP TABLE #PAGE_ALLOCATION_CONTENTION
    
      
      CREATE TABLE #PAGE_ALLOCATION_CONTENTION
         ( BATCH_ID INT IDENTITY(1, 1)
         , BATCH_DATETIME DATETIME
         , TASK_COUNT INT
         )

       DECLARE @PAGE_ALLOC_COUNTER INT = 1
       WHILE @PAGE_ALLOC_COUNTER <= @PAGE_ALLOCATION_CONTENTION_LOOPS
         BEGIN
            INSERT INTO #PAGE_ALLOCATION_CONTENTION
               SELECT GETDATE(), COUNT(1)
               FROM SYS.DM_OS_WAITING_TASKS
               WHERE WAIT_TYPE IN ('PAGELATCH', 'PAGEIOLATCH')
               AND (RESOURCE_ADDRESS LIKE '2:%:1' OR RESOURCE_ADDRESS LIKE '2:%:3')
            
            SET @PAGE_ALLOC_COUNTER = @PAGE_ALLOC_COUNTER + 1
 
            WAITFOR DELAY @PAGE_ALLOCATION_CONTENTION_SECS_PER
         END -- WHILE LOOP    

   END  --  IF @SHOW_PAGE_ALLOCATION_CONTENTION = 1  

------------------------------------------------------------------------------------------------------------------------------------
/* SESSION-LEVEL OUTPUTS*/
------------------------------------------------------------------------------------------------------------------------------------

IF @INCLUDE_SESSION_INFO = 1
BEGIN
 IF OBJECT_ID(N'TEMPDB..#SESSION') IS NOT NULL DROP TABLE #SESSION
  CREATE TABLE #SESSION
         (SESSION_ID                  SMALLINT      
         , BLOCKING_SESSION_ID        SMALLINT
         , TRANSACTION_COUNT          TINYINT
         , DB_NAME                    NVARCHAR(128)
         , STATUS                     NVARCHAR(300)
         , LOGIN_TIME                 DATETIME
         , REQ_WAIT_TYPE              NVARCHAR(60)
         , REQ_COMMAND                NVARCHAR(160)
         , SQL_STATEMENT              NVARCHAR(MAX)
		   , CURSOR_STATEMENT			  NVARCHAR(MAX)
         , OBJECT_ID                  INT
         , OBJECT_NAME                NVARCHAR(128)


         , HOST_NAME                  NVARCHAR(128)
         , PROGRAM_NAME               NVARCHAR(128)
         , CLIENT_INTERFACE_NAME      NVARCHAR(320)
         , LOGIN_NAME                 NVARCHAR(128)
         , SESSION_TOTAL_ELAPSED_TIME INT
         , LAST_REQUEST_START_TIME    DATETIME
         , LAST_REQUEST_END_TIME      DATETIME
         , REQ_START_TIME             DATETIME
         , REQ_TOTAL_ELAPSED_TIME     INT
         , REQ_WAIT_TIME              INT
         , REQ_LAST_WAIT_TYPE         NVARCHAR(600)
         , REQ_ROW_COUNT BIGINT
		   , CURSOR_NAME				     NVARCHAR(256)
		   , CURSOR_PROPERTIES		     NVARCHAR(256)
		   , CURSOR_CREATION_DATETIME   DATETIME
		   , CURSOR_IS_OPEN			     BIT
		   , CURSOR_IS_ASYNC_POPULATION BIT
         , CURSOR_IS_CLOSE_ON_COMMIT  BIT
         , CURSOR_FETCH_STATUS        INT
         , CURSOR_WORKER_TIME         BIGINT
         , CURSOR_READS               BIGINT
         , CURSOR_WRITES              BIGINT
         , CURSOR_DORMANT_DURATION    BIGINT

         , PERCENT_COMPLETE           REAL
         , ESTIMATED_COMPLETION_TIME  BIGINT
         , TRANSACTION_ISOLATION_LEVEL SMALLINT
         , CONCAT_NULL_YIELDS_NULL BIT
         , ARITHABORT BIT
         , ANSI_PADDING BIT
         , ANSI_NULLS BIT
         , DEADLOCK_PRIORITY INT
         , NEST_LEVEL INT
         --, QUERY_PLAN XML

         )
   /*PULL INFO FOR ALL TRANSACTIONS*/
   INSERT INTO #SESSION
      SELECT S.SESSION_ID
         , R.BLOCKING_SESSION_ID
         , null as OPEN_TRANSACTION_COUNT
         , D.NAME AS DB_NAME
         , S.STATUS
         , S.LOGIN_TIME
         , R.WAIT_TYPE AS REQ_WAIT_TYPE
         , R.COMMAND AS REQ_COMMAND
         ,     (SELECT TOP 1 SUBSTRING(S2.TEXT,R.STATEMENT_START_OFFSET / 2+1 , 
               ( (CASE WHEN R.STATEMENT_END_OFFSET = -1 
                  THEN (LEN(CONVERT(NVARCHAR(MAX),S2.TEXT)) * 2) 
                  ELSE R.STATEMENT_END_OFFSET END)  - R.STATEMENT_START_OFFSET) / 2+1))  AS SQL_STATEMENT
	   	,   (SELECT TOP 1 SUBSTRING(S2.TEXT, C.STATEMENT_START_OFFSET / 2+1 , 
               ( (CASE WHEN C.STATEMENT_END_OFFSET = -1 
                  THEN (LEN(CONVERT(NVARCHAR(MAX),S2.TEXT)) * 2) 
                  ELSE C.STATEMENT_END_OFFSET END)  - C.STATEMENT_START_OFFSET) / 2+1))  AS CURSOR_STATEMENT
         , S2.OBJECTID AS OBJECT_ID
         , OBJECT_NAME(S2.OBJECTID, S2.dbid) AS OBJECT_NAME
         , S.HOST_NAME
         , S.PROGRAM_NAME
         , S.CLIENT_INTERFACE_NAME
         , S.LOGIN_NAME
         , S.TOTAL_ELAPSED_TIME      AS SESSION_TOTAL_ELAPSED_TIME
         , S.LAST_REQUEST_START_TIME
         , S.LAST_REQUEST_END_TIME
         , R.START_TIME              AS REQ_START_TIME
         , R.TOTAL_ELAPSED_TIME      AS REQ_TOTAL_ELAPSED_TIME
         , R.WAIT_TIME               AS REQ_WAIT_TIME
         , R.LAST_WAIT_TYPE          AS REQ_LAST_WAIT_TYPE
         , R.ROW_COUNT               AS REQ_ROW_COUNT

		   , C.NAME				          AS CURSOR_NAME				     
		   , C.PROPERTIES		          AS CURSOR_PROPERTIES		     
		   , C.CREATION_TIME           AS CURSOR_CREATION_DATETIME   
		   , C.IS_OPEN			          AS CURSOR_IS_OPEN			     
		   , C.IS_ASYNC_POPULATION     AS CURSOR_IS_ASYNC_POPULATION 
         , C.IS_CLOSE_ON_COMMIT      AS CURSOR_IS_CLOSE_ON_COMMIT  
         , C.FETCH_STATUS            AS CURSOR_FETCH_STATUS        
         , C.WORKER_TIME             AS CURSOR_WORKER_TIME         
         , C.READS                   AS CURSOR_READS               
         , C.WRITES                  AS CURSOR_WRITES              
         , C.DORMANT_DURATION        AS CURSOR_DORMANT_DURATION    

         , R.PERCENT_COMPLETE
         , R.ESTIMATED_COMPLETION_TIME
         , R.TRANSACTION_ISOLATION_LEVEL
         , R.CONCAT_NULL_YIELDS_NULL
         , R.ARITHABORT
         , R.ANSI_PADDING
         , R.ANSI_NULLS
         , R.DEADLOCK_PRIORITY
         , R.NEST_LEVEL
      FROM SYS.DM_EXEC_SESSIONS                        AS  S
      LEFT JOIN SYS.DM_EXEC_REQUESTS                   AS  R ON R.SESSION_ID = S.SESSION_ID
      LEFT JOIN SYS.DATABASES                          AS  D ON D.DATABASE_ID = R.DATABASE_ID
      OUTER APPLY SYS.DM_EXEC_SQL_TEXT (R.SQL_HANDLE)  AS S2
	   OUTER APPLY SYS.DM_EXEC_CURSORS  (S.SESSION_ID)  AS  C 
	   OUTER APPLY SYS.DM_EXEC_SQL_TEXT (C.SQL_HANDLE)  AS S3
      WHERE S.SESSION_ID = @SESSION_ID OR @SESSION_ID IS NULL

      IF OBJECT_ID(N'TEMPDB..#ACTIVE_TEMPDB_TRANSACTIONS') IS NOT NULL DROP TABLE #ACTIVE_TEMPDB_TRANSACTIONS

      SELECT * 
      INTO #ACTIVE_TEMPDB_TRANSACTIONS
      FROM SYS.DM_TRAN_ACTIVE_SNAPSHOT_DATABASE_TRANSACTIONS
      WHERE @SESSION_ID IS NULL OR SESSION_ID = @SESSION_ID

 
------------------------------------------------------------------------------------------------------------------------------------

      IF OBJECT_ID(N'TEMPDB..#SESSION_SPACE_USAGE') IS NOT NULL DROP TABLE #SESSION_SPACE_USAGE

      SELECT SESSION_ID
      , DATABASE_ID
      , USER_OBJECTS_ALLOC_PAGE_COUNT
      , USER_OBJECTS_DEALLOC_PAGE_COUNT
      , USER_OBJECTS_ALLOC_PAGE_COUNT - USER_OBJECTS_DEALLOC_PAGE_COUNT AS OUTSTANDING_USER_ALLOC_PAGE_COUNT
      , INTERNAL_OBJECTS_ALLOC_PAGE_COUNT
      , INTERNAL_OBJECTS_DEALLOC_PAGE_COUNT
      , INTERNAL_OBJECTS_ALLOC_PAGE_COUNT - INTERNAL_OBJECTS_DEALLOC_PAGE_COUNT AS OUTSTANDING_INTERNAL_ALLOC_PAGE_COUNT
      ,  (USER_OBJECTS_ALLOC_PAGE_COUNT - USER_OBJECTS_DEALLOC_PAGE_COUNT )
       + (INTERNAL_OBJECTS_ALLOC_PAGE_COUNT - INTERNAL_OBJECTS_DEALLOC_PAGE_COUNT) AS TOTAL_OUTSTANDING_ALLOC_PAGE_COUNT
      INTO #SESSION_SPACE_USAGE
      FROM SYS.DM_DB_SESSION_SPACE_USAGE
      WHERE @SESSION_ID IS NULL OR SESSION_ID = @SESSION_ID

  
----------------------------------------------------------------------------------------

      SELECT '@SINGLE_OUTPUT_BY_SESSION_ID (ACTIVE TRANSACTION + SESSION SPACE USAGE), WITH SESSION INFO' AS OUTPUT
      , COALESCE(A.SESSION_ID, U.SESSION_ID) AS SESSION_ID
      , COALESCE(X.TRANSACTION_COUNT , X2.TRANSACTION_COUNT ) AS OPEN_TRANSACTIONS                                
      ,  COALESCE(X.HOST_NAME                    , X2.HOST_NAME                ) AS HOST_NAME                   
      , A.ELAPSED_TIME_SECONDS                                                   AS CURRENT_REQ_TIME
      , COALESCE(X.STATUS                       , X2.STATUS                    ) AS STATUS                                   
      , COALESCE(X.LOGIN_TIME                   , X2.LOGIN_TIME                ) AS LOGIN_TIME                            
      , COALESCE(X.LAST_REQUEST_END_TIME        , X2.LAST_REQUEST_END_TIME     ) AS LAST_REQUEST_END_TIME       
      , COALESCE(X.REQ_START_TIME               , X2.REQ_START_TIME            ) AS REQ_START_TIME              
      , U.TOTAL_OUTSTANDING_ALLOC_PAGE_COUNT 
      , U.USER_OBJECTS_ALLOC_PAGE_COUNT 
      , U.USER_OBJECTS_DEALLOC_PAGE_COUNT 
      , U.OUTSTANDING_USER_ALLOC_PAGE_COUNT 
      , U.INTERNAL_OBJECTS_ALLOC_PAGE_COUNT 
      , U.INTERNAL_OBJECTS_DEALLOC_PAGE_COUNT 
      , U.OUTSTANDING_INTERNAL_ALLOC_PAGE_COUNT 
      , A.MAX_VERSION_CHAIN_TRAVERSED 
      , A.AVERAGE_VERSION_CHAIN_TRAVERSED 
      , COALESCE(X.PROGRAM_NAME                 , X2.PROGRAM_NAME              ) AS PROGRAM_NAME                
      , COALESCE(X.BLOCKING_SESSION_ID          , X2.BLOCKING_SESSION_ID       ) AS BLOCKING_SESSION_ID            
      , COALESCE(X.DB_NAME                      , X2.DB_NAME                   ) AS DB_NAME                                  
      , COALESCE(X.REQ_WAIT_TYPE                , X2.REQ_WAIT_TYPE             ) AS REQ_WAIT_TYPE                         
      , COALESCE(X.REQ_COMMAND                  , X2.REQ_COMMAND               ) AS REQ_COMMAND                          
      , COALESCE(X.SQL_STATEMENT                , X2.SQL_STATEMENT             ) AS SQL_STATEMENT 
      , COALESCE(X.CURSOR_STATEMENT             , X2.CURSOR_STATEMENT          ) AS CURSOR_STATEMENT                       
      , COALESCE(X.OBJECT_ID                    , X2.OBJECT_ID                 ) AS OBJECT_ID                          
      , COALESCE(X.OBJECT_NAME                  , X2.OBJECT_NAME               ) AS OBJECT_NAME                       
      , COALESCE(X.PERCENT_COMPLETE             , X2.PERCENT_COMPLETE          ) AS PERCENT_COMPLETE                 
      , COALESCE(X.ESTIMATED_COMPLETION_TIME    , X2.ESTIMATED_COMPLETION_TIME ) AS ESTIMATED_COMPLETION_TIME   
      , COALESCE(X.CLIENT_INTERFACE_NAME        , X2.CLIENT_INTERFACE_NAME     ) AS CLIENT_INTERFACE_NAME       
      , COALESCE(X.LOGIN_NAME                   , X2.LOGIN_NAME                ) AS LOGIN_NAME                  
      , COALESCE(X.SESSION_TOTAL_ELAPSED_TIME   , X2.SESSION_TOTAL_ELAPSED_TIME) AS SESSION_TOTAL_ELAPSED_TIME  
      , COALESCE(X.LAST_REQUEST_START_TIME      , X2.LAST_REQUEST_START_TIME   ) AS LAST_REQUEST_START_TIME     
      , COALESCE(X.REQ_TOTAL_ELAPSED_TIME       , X2.REQ_TOTAL_ELAPSED_TIME    ) AS REQ_TOTAL_ELAPSED_TIME      
      , COALESCE(X.REQ_WAIT_TIME                , X2.REQ_WAIT_TIME             ) AS REQ_WAIT_TIME   


      , COALESCE(X.CURSOR_NAME				      , X.CURSOR_NAME			       )  AS CURSOR_NAME				   	   
      , COALESCE(X.CURSOR_PROPERTIES		      , X.CURSOR_PROPERTIES		    )  AS CURSOR_PROPERTIES		   
      , COALESCE(X.CURSOR_CREATION_DATETIME     , X.CURSOR_CREATION_DATETIME   )  AS CURSOR_CREATION_DATETIME  
      , COALESCE(X.CURSOR_IS_OPEN			      , X.CURSOR_IS_OPEN			    )  AS CURSOR_IS_OPEN			   
      , COALESCE(X.CURSOR_IS_ASYNC_POPULATION   , X.CURSOR_IS_ASYNC_POPULATION )  AS CURSOR_IS_ASYNC_POPULATION
      , COALESCE(X.CURSOR_IS_CLOSE_ON_COMMIT    , X.CURSOR_IS_CLOSE_ON_COMMIT  )  AS CURSOR_IS_CLOSE_ON_COMMIT 
      , COALESCE(X.CURSOR_FETCH_STATUS          , X.CURSOR_FETCH_STATUS        )  AS CURSOR_FETCH_STATUS       
      , COALESCE(X.CURSOR_WORKER_TIME           , X.CURSOR_WORKER_TIME         )  AS CURSOR_WORKER_TIME        
      , COALESCE(X.CURSOR_READS                 , X.CURSOR_READS               )  AS CURSOR_READS              
      , COALESCE(X.CURSOR_WRITES                , X.CURSOR_WRITES              )  AS CURSOR_WRITES             
      , COALESCE(X.CURSOR_DORMANT_DURATION      , X.CURSOR_DORMANT_DURATION    )  AS CURSOR_DORMANT_DURATION   
      , A.TRANSACTION_ID
      , A.TRANSACTION_SEQUENCE_NUM 
      , A.COMMIT_SEQUENCE_NUM 
      , A.IS_SNAPSHOT 
      , A.FIRST_SNAPSHOT_SEQUENCE_NUM 
--       , COALESCE(X.QUERY_PLAN                   , X2.QUERY_PLAN  )               AS QUERY_PLAN          
      FROM #ACTIVE_TEMPDB_TRANSACTIONS A
      FULL JOIN #SESSION_SPACE_USAGE  U ON U.SESSION_ID = A.SESSION_ID 
      LEFT JOIN #SESSION X ON X.SESSION_ID = A.SESSION_ID
      LEFT JOIN #SESSION X2 ON X2.SESSION_ID = U.SESSION_ID
      ORDER BY ELAPSED_TIME_SECONDS DESC
            , TOTAL_OUTSTANDING_ALLOC_PAGE_COUNT DESC

   END  --@INCLUDE_SESSION_INFO = 1


--------------------------------------------------------------------------------------------------------------------------
/* OUTPUT - LOCKS ON OBJECTS IN TEMPDB*/
--------------------------------------------------------------------------------------------------------------------------

IF @SHOW_LOCKS_IN_TEMPDB = 1
   BEGIN

      IF OBJECT_ID(N'TEMPDB..#LOCKS') IS NOT NULL DROP TABLE #LOCKS
 
      CREATE TABLE #LOCKS
      (
            OUTPUT               varchar(21)
          , REQUEST_SESSION_ID   int
          , RESOURCE_DATABASE_ID int
          , RESOURCE_TYPE        nvarchar(60)
          , REQUEST_MODE         nvarchar(60)
          , REQUEST_STATUS       nvarchar(60)
          , ROW_COUNT            int
      )
  
      INSERT INTO #LOCKS
         SELECT '@SHOW_LOCKS_IN_TEMPDB' AS OUTPUT, REQUEST_SESSION_ID, RESOURCE_DATABASE_ID, RESOURCE_TYPE, REQUEST_MODE, REQUEST_STATUS, COUNT(*) AS ROW_COUNT
         FROM SYS.DM_TRAN_LOCKS
         WHERE NOT(RESOURCE_TYPE = 'DATABASE' AND REQUEST_MODE = 'S')
         AND RESOURCE_DATABASE_ID = 2
         GROUP BY REQUEST_SESSION_ID, RESOURCE_DATABASE_ID, RESOURCE_TYPE, REQUEST_MODE, REQUEST_STATUS

      IF @@ROWCOUNT = 1
         BEGIN
            SELECT * 
            FROM #LOCKS 
            ORDER BY RESOURCE_DATABASE_ID, REQUEST_SESSION_ID, RESOURCE_TYPE, REQUEST_MODE, REQUEST_STATUS
         END
      ELSE
         BEGIN
            SELECT '@SHOW_LOCKS_IN_TEMPDB'AS OUTPUT
             , NULL AS REQUEST_SESSION_ID
             , NULL AS RESOURCE_DATABASE_ID
             , NULL AS RESOURCE_TYPE
             , NULL AS REQUEST_MODE
             , NULL AS REQUEST_STATUS
             , NULL AS ROW_COUNT
         END

         
   END --IF @SHOW_LOCKS_IN_TEMPDB = 1

--------------------------------------------------------------------------------------------------------------------------
/* OUTPUT - USER TABLES IN TEMPDB*/
--------------------------------------------------------------------------------------------------------------------------

IF @SHOW_USER_TABLES_IN_TEMPDB = 1
BEGIN

/* COLLECT DATA - GET THE BASIC SET OF TEMPDB OBJECTS*/

      IF OBJECT_ID(N'TEMPDB..#JJK_TMPTBL_VALUES') IS NOT NULL DROP TABLE #JJK_TMPTBL_VALUES
      -- TEMPORARY TABLES AND THEIR SIZE 
      SELECT  TBL.NAME AS OBJNAME    
            , TBL.OBJECT_ID
            , CASE WHEN TBL.OBJECT_ID < 0 AND TBL.NAME NOT LIKE '#[AB][0-9, A-Z]%'
                   THEN 'TEMP TABLE'
                   WHEN TBL.OBJECT_ID < 0 AND TBL.NAME LIKE '#[AB][0-9, A-Z]%'
                   THEN 'T VARIABLE/TVP/TVF/IF'
                   WHEN TBL.OBJECT_ID >=0 AND TBL.NAME LIKE '##%' THEN 'GLOBAL TEMP TABLE' 
                   ELSE tbl.type_desc END as object_type
            , CHARINDEX('___', TBL.NAME) AS START_UNDERSCORE
            , TBL.CREATE_DATE
            ,STAT.ROW_COUNT AS ROW_COUNT 
            ,STAT.USED_PAGE_COUNT * 8 AS USED_SIZE_KB 
            ,STAT.RESERVED_PAGE_COUNT * 8 AS REVERVED_SIZE_KB 
            , STAT.USED_PAGE_COUNT
            , STAT.RESERVED_PAGE_COUNT
            , PART.HOBT_ID
            , PART.PARTITION_ID 
      INTO #JJK_TMPTBL_VALUES
      FROM TEMPDB.SYS.PARTITIONS (NOLOCK) AS PART 
           INNER JOIN TEMPDB.SYS.DM_DB_PARTITION_STATS (NOLOCK) AS STAT  ON PART.PARTITION_ID = STAT.PARTITION_ID 
                                                                   AND PART.PARTITION_NUMBER = STAT.PARTITION_NUMBER 
           INNER JOIN TEMPDB.SYS.objects (NOLOCK)  AS TBL  ON STAT.OBJECT_ID = TBL.OBJECT_ID 
   --   WHERE TBL.NAME NOT LIKE '#[AB][0-9, A-Z]%'
              -- AND LEN(TBL.NAME) <> 9
         WHERE (TBL.TYPE_DESC = 'SYSTEM_TABLE' AND  @INCLUDE_SYSTEM_TABLES  = 1) 
            OR (TBL.TYPE_DESC = 'INTERNAL_TABLE' AND @INCLUDE_INTERNAL_TABLES = 1)
            OR (TBL.TYPE_DESC NOT IN ( 'SYSTEM_TABLE', 'INTERNAL_TABLE'))
  
             
/* COLLECT DATA - PULL PAGE ALLOCATIONS AND BUFFER DESCRIPTORS IF CACHED VS SPOOLED ANALYSIS WANTED */

IF @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1
   BEGIN
      IF OBJECT_ID(N'TEMPDB..#ALLOCATIONS') IS NOT NULL DROP TABLE #ALLOCATIONS
      SELECT P.OBJECT_ID
         , P.OBJNAME
         , AU.ALLOCATION_UNIT_ID
         , AU.DATA_PAGES
         , USED_PAGES
         , TOTAL_PAGES
      INTO #ALLOCATIONS
      FROM SYS.ALLOCATION_UNITS     (NOLOCK) AS AU
      INNER JOIN #JJK_TMPTBL_VALUES (NOLOCK) AS P ON AU.CONTAINER_ID = P.HOBT_ID AND (AU.TYPE = 1 OR AU.TYPE = 3)
      UNION ALL
      SELECT P.OBJECT_ID
         , P.OBJNAME
         , AU.ALLOCATION_UNIT_ID
         , AU.DATA_PAGES
         , USED_PAGES
         , TOTAL_PAGES
      FROM SYS.ALLOCATION_UNITS     (NOLOCK) AS AU
      INNER JOIN #JJK_TMPTBL_VALUES (NOLOCK) AS P ON AU.CONTAINER_ID = P.PARTITION_ID AND (AU.TYPE = 2)


      IF OBJECT_ID(N'TEMPDB..#BUFFER') IS NOT NULL DROP TABLE #BUFFER

      SELECT
           OBJECT_ID
         , FILE_ID
         ,  COUNT(*)AS CACHED_PAGE_COUNT 
      INTO #BUFFER
      FROM SYS.DM_OS_BUFFER_DESCRIPTORS (NOLOCK) AS  BD 
          INNER JOIN #ALLOCATIONS          OBJ  ON BD.ALLOCATION_UNIT_ID = OBJ.ALLOCATION_UNIT_ID
      WHERE DATABASE_ID = DB_ID()
      GROUP BY OBJECT_ID, FILE_ID
   END --IF @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1


/* COLLECT DATA - PULL PAGE ALLOCATIONS IF FILE_BREAKOUT DESIRED*/
   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1
      BEGIN
         IF OBJECT_ID(N'TEMPDB..#PAGE_ALLOCATIONS') IS NOT NULL DROP TABLE #PAGE_ALLOCATIONS

         SELECT *
         INTO #PAGE_ALLOCATIONS
         FROM SYS.DM_DB_DATABASE_PAGE_ALLOCATIONS(2, NULL, NULL, NULL, 'LIMITED')  NOLOCK

         IF OBJECT_ID(N'TEMPDB..#OBJECT_FILE') IS NOT NULL DROP TABLE #OBJECT_FILE

         SELECT OBJECT_ID
            , EXTENT_FILE_ID AS FILE_ID
            , COUNT(*) AS PAGE_COUNT
        INTO #OBJECT_FILE
        FROM #PAGE_ALLOCATIONS
        GROUP BY  OBJECT_ID
            , EXTENT_FILE_ID
      END --    IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1



/* RETURN DATA*/
--------------------------------------------------------------------------------------------------------------------
/* objects.  no file breakout.  no cached vs spooled.*/
--------------------------------------------------------------------------------------------------------------------
   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 AND @SHOW_DETAIL = 1
      BEGIN
         SELECT 'Detail - @SHOW_USER_TABLES_IN_TEMPDB.  No File Breakout.  No cached vs spooled' AS OUTPUT
            , CASE WHEN START_UNDERSCORE = 0 THEN OBJNAME 
                      ELSE SUBSTRING(OBJNAME, 1, START_UNDERSCORE - 1 ) END AS OBJNAME
            , OBJECT_ID
            , OBJECT_TYPE
            , CREATE_DATE 
            , ROW_COUNT 
            , USED_SIZE_KB 
            , USED_PAGE_COUNT
            , REVERVED_SIZE_KB  
            , RESERVED_PAGE_COUNT
         FROM #JJK_TMPTBL_VALUES
         ORDER BY ROW_COUNT DESC
      END --IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0  AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_detail = 1

   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 AND @SHOW_SUMMARY = 1
      BEGIN
         SELECT 'Summary - @SHOW_USER_TABLES_IN_TEMPDB.  No file breakout.  no cached vs spooled' AS OUTPUT
            , OBJECT_TYPE
            , count(*)                 as OBJECT_COUNT
            , SUM(ROW_COUNT          ) AS ROW_COUNT   
            , SUM(USED_SIZE_KB       ) AS USED_SIZE_KB 
            , SUM(USED_PAGE_COUNT    ) AS USED_PAGE_COUNT
            , SUM(REVERVED_SIZE_KB   ) AS REVERVED_SIZE_KB  
            , SUM(RESERVED_PAGE_COUNT) AS RESERVED_PAGE_COUNT
         FROM #JJK_TMPTBL_VALUES
         GROUP BY OBJECT_TYPE
         ORDER BY OBJECT_TYPE
      END --IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0  AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_summary = 1

--------------------------------------------------------------------------------------------------------------------
/* objects.   file breakout.  no cached vs spooled.*/
--------------------------------------------------------------------------------------------------------------------

   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_detail = 1
      BEGIN
         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB + @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES - @INCLUDE_CACHED_VS_SPOOLED_PAGES' AS OUTPUT
                  , TBLS.OBJECT_ID
                  , CASE WHEN START_UNDERSCORE = 0 THEN OBJNAME 
                         ELSE SUBSTRING(OBJNAME, 1, START_UNDERSCORE - 1 ) END AS OBJNAME
                  , OBJECT_TYPE
                  , CREATE_DATE 
                  , ROW_COUNT 
                  , USED_SIZE_KB 
                  , REVERVED_SIZE_KB  
                  , USED_PAGE_COUNT
                  , RESERVED_PAGE_COUNT
                  , FILE_ID
                  , PAGE_COUNT AS FILE_PAGE_COUNT
         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #OBJECT_FILE PAGES ON PAGES.OBJECT_ID = TBLS.OBJECT_ID
      --   WHERE OBJNAME NOT LIKE '#[AB][0-9, A-Z]%'
              -- AND LEN(OBJNAME) <> 9
         ORDER BY  OBJNAME
                  , OBJECT_ID
                  , FILE_ID
      END --   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_detail = 1



   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_SUMMARY = 1
      BEGIN
         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB - detail. file breakout, no cache vs spool' AS OUTPUT
            , TBLS.OBJECT_TYPE
            , FILE_ID
            , count(*)                 as OBJECT_COUNT
            , SUM(ROW_COUNT          ) AS ROW_COUNT   
            , SUM(USED_SIZE_KB       ) AS USED_SIZE_KB 
            , SUM(USED_PAGE_COUNT    ) AS USED_PAGE_COUNT
            , SUM(REVERVED_SIZE_KB   ) AS REVERVED_SIZE_KB  
            , SUM(RESERVED_PAGE_COUNT) AS RESERVED_PAGE_COUNT
            , sum(PAGE_COUNT         ) AS FILE_PAGE_COUNT
         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #OBJECT_FILE PAGES ON PAGES.OBJECT_ID = TBLS.OBJECT_ID
      --   WHERE OBJNAME NOT LIKE '#[AB][0-9, A-Z]%'
              -- AND LEN(OBJNAME) <> 9
         GROUP BY  TBLS.OBJECT_TYPE
                  , FILE_ID         
         ORDER BY  TBLS.OBJECT_TYPE
                  , FILE_ID
      END --   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 0 and @show_summary = 1

--------------------------------------------------------------------------------------------------------------------
/* objects.   cached vs spooled. NO file breakout. */
--------------------------------------------------------------------------------------------------------------------

   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 AND @SHOW_DETAIL = 1
      BEGIN
         IF OBJECT_ID(N'TEMPDB..#BUFFER2') IS NOT NULL DROP TABLE #BUFFER2

         SELECT OBJECT_ID, SUM(CACHED_PAGE_COUNT) AS CACHED_PAGE_COUNT
         INTO #BUFFER2
         FROM #BUFFER      
         GROUP BY OBJECT_ID

         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB - detail.  cached vs spooled.  no file breakout' AS OUTPUT
         , TBLS.OBJECT_ID
         , CASE WHEN START_UNDERSCORE = 0 THEN OBJNAME 
                ELSE SUBSTRING(OBJNAME, 1, START_UNDERSCORE - 1 ) END AS OBJNAME
         , OBJECT_TYPE
         , CREATE_DATE 
         , ROW_COUNT 
         , USED_SIZE_KB 
         , REVERVED_SIZE_KB  
         , USED_PAGE_COUNT
         , RESERVED_PAGE_COUNT
         , B.CACHED_PAGE_COUNT
         , RESERVED_PAGE_COUNT - B.CACHED_PAGE_COUNT AS PAGES_SPOOLED_TO_DISK
         , CASE WHEN COALESCE(RESERVED_PAGE_COUNT, 0) = 0 THEN 0 
                  ELSE CAST( B.CACHED_PAGE_COUNT*1.0/RESERVED_PAGE_COUNT AS DECIMAL(6, 2)) END AS CACHED_PCT
         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #BUFFER2 B ON B.OBJECT_ID = TBLS.OBJECT_ID 
         ORDER BY  object_type  
                  , OBJNAME
                  , OBJECT_ID

    END -- IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 AND @SHOW_DETAIL = 1


   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 0 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 AND @SHOW_SUMMARY = 1
      BEGIN     
         IF OBJECT_ID(N'TEMPDB..#BUFFER3') IS NOT NULL DROP TABLE #BUFFER3

         SELECT OBJECT_ID, SUM(CACHED_PAGE_COUNT) AS CACHED_PAGE_COUNT
         INTO #BUFFER3
         FROM #BUFFER      
         GROUP BY OBJECT_ID

         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB - summary.  cached vs spooled.  no file breakout' AS OUTPUT
            , OBJECT_TYPE
            , count(*)                 as OBJECT_COUNT
            , SUM(ROW_COUNT          ) AS ROW_COUNT   
            , SUM(USED_SIZE_KB       ) AS USED_SIZE_KB 
            , SUM(USED_PAGE_COUNT    ) AS USED_PAGE_COUNT
            , SUM(REVERVED_SIZE_KB   ) AS REVERVED_SIZE_KB  
            , SUM(RESERVED_PAGE_COUNT) AS RESERVED_PAGE_COUNT

            , SUM(B.CACHED_PAGE_COUNT) AS CACHED_PAGE_COUNT
            , SUM(RESERVED_PAGE_COUNT) - sum(B.CACHED_PAGE_COUNT)  AS PAGES_SPOOLED_TO_DISK
         , CASE WHEN COALESCE(SUM(RESERVED_PAGE_COUNT), 0) = 0 THEN 0 
                  ELSE CAST( SUM(B.CACHED_PAGE_COUNT)*1.0/SUM(RESERVED_PAGE_COUNT) AS DECIMAL(6, 2)) END AS CACHED_PCT

         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #BUFFER3 B ON B.OBJECT_ID = TBLS.OBJECT_ID 
         GROUP BY  TBLS.OBJECT_TYPE
         ORDER BY  TBLS.OBJECT_TYPE
    END -- IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 AND @SHOW_SUMMARY = 1

--------------------------------------------------------------------------------------------------------------------
/* objects.   file breakout.  cached vs spooled.*/
--------------------------------------------------------------------------------------------------------------------


   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 and @SHOW_DETAIL = 1
      BEGIN
         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB - detail.  File breakout and cache vs spool.' AS OUTPUT
         , TBLS.OBJECT_ID
         , CASE WHEN START_UNDERSCORE = 0 THEN OBJNAME 
                ELSE SUBSTRING(OBJNAME, 1, START_UNDERSCORE - 1 ) END AS OBJNAME
         , OBJECT_TYPE
         , CREATE_DATE 
         , ROW_COUNT 
         , USED_SIZE_KB 
         , REVERVED_SIZE_KB  
         , USED_PAGE_COUNT
         , RESERVED_PAGE_COUNT
         , PAGES.FILE_ID
         , PAGE_COUNT AS FILE_PAGE_COUNT
         , B.CACHED_PAGE_COUNT
         , PAGE_COUNT - B.CACHED_PAGE_COUNT AS PAGES_SPOOLED_TO_DISK
         , CASE WHEN COALESCE(PAGE_COUNT, 0) = 0 THEN 0 
                  ELSE CAST( B.CACHED_PAGE_COUNT*1.0/PAGE_COUNT AS DECIMAL(6, 2)) END AS CACHED_PCT
         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #OBJECT_FILE PAGES ON PAGES.OBJECT_ID = TBLS.OBJECT_ID
         LEFT JOIN #BUFFER B ON B.OBJECT_ID = TBLS.OBJECT_ID AND B.FILE_ID = PAGES.FILE_ID
         ORDER BY  object_type  
                  , OBJNAME
                  , OBJECT_ID
                  , FILE_ID
      END --    IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 and @SHOW_DETAIL = 1


   IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 and @SHOW_SUMMARY = 1
      BEGIN
         SELECT  '@SHOW_USER_TABLES_IN_TEMPDB - summary.  File breakout and cache vs spool.' AS OUTPUT
            , TBLS.OBJECT_TYPE
            , PAGES.FILE_ID
            , count(*)                 as OBJECT_COUNT
            , SUM(ROW_COUNT          ) AS ROW_COUNT   
            , SUM(USED_SIZE_KB       ) AS USED_SIZE_KB 
            , SUM(USED_PAGE_COUNT    ) AS USED_PAGE_COUNT
            , SUM(REVERVED_SIZE_KB   ) AS REVERVED_SIZE_KB  
            , SUM(RESERVED_PAGE_COUNT) AS RESERVED_PAGE_COUNT

            , SUM(PAGES.PAGE_COUNT   ) AS FILE_PAGE_COUNT

            , SUM(B.CACHED_PAGE_COUNT) AS CACHED_PAGE_COUNT
            , SUM(RESERVED_PAGE_COUNT) - sum(B.CACHED_PAGE_COUNT)  AS PAGES_SPOOLED_TO_DISK
         , CASE WHEN COALESCE(SUM(PAGES.PAGE_COUNT), 0) = 0 THEN 0 
                  ELSE CAST( SUM(B.CACHED_PAGE_COUNT)*1.0/SUM(PAGES.PAGE_COUNT) AS DECIMAL(6, 2)) END AS CACHED_PCT
         FROM #JJK_TMPTBL_VALUES TBLS
         LEFT JOIN #OBJECT_FILE PAGES ON PAGES.OBJECT_ID = TBLS.OBJECT_ID
         LEFT JOIN #BUFFER B ON B.OBJECT_ID = TBLS.OBJECT_ID AND B.FILE_ID = PAGES.FILE_ID
         GROUP BY  TBLS.OBJECT_TYPE
                  , PAGES.FILE_ID
         ORDER BY  TBLS.OBJECT_TYPE
                  , FILE_ID
      END --    IF @INCLUDE_FILE_BREAKOUT_FOR_USER_TABLES = 1 AND @INCLUDE_CACHED_VS_SPOOLED_PAGES = 1 and @SHOW_DETAIL = 1

END --IF @SHOW_USER_TABLES_IN_TEMPDB = 1

