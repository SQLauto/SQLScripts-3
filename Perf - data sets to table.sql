/*
SCRIPT - PLAN CACHE ANALYSIS 
AUTHOR - JOHN KAUFFMAN, HTTP://SQLJOHNKAUFFMAN.WORDPRESS.COM/

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

/* this script assumes that the plan cache and wait stats are flushed between each execution being tested
*/



use tempdb

declare @data_set varchar(100) = 'DS - 50'
declare @run_type varchar(100) = 'baseline'
declare @run_nbr  int = 3

/* if this run number exists, prompt for parameter update*/
if exists (select * from tempdb.sys.tables where name = 'plan_cache_stmt_details')
   begin
      if exists (select * from tempdb..plan_cache_stmt_details
                  where data_set = @data_set
                      and run_type = @run_type
                      and run_nbr = @run_nbr)
         begin
           raiserror('Entries for this run number already exist.  Increment the run number and re-run.', 10, 1) with nowait
           goto skipToEnd
         end
   end


/* THIS VERSION OF THE PLAN CACHE QUERY HAS SEVERAL GOALS:
1.  TO PULL METADATA ABOUT THE PLAN CACHE, NOT JUST DETAILS ABOUT THE SPECIFIC QUERIES.
2.  TO WORK ACROSS ALL DATABASES.  IT DOES SO BY GETTING SYS.OBJECTS FROM ALL DBS.
3.  TO PULL ALL PROCS, NOT JUST A SINGLE PROC.  THIS REQUIRES BETTER COORDINATION BETWEEN THE PROC LEVEL AND QUERY LEVEL OUTPUTS
4.  TO PROVIDE INFORMATION ABOUT AD HOC DATA.  PERFORMANCE DATA ARE AGGREGATED BY QUERY HASH AND QUERY PLAN HASH TO SHOW THE IMPACT OF THE STATEMENT.
5.  TO RANK PROCS AND AD HOC STATEMENTS BY VARIOUS FACTORS SO THAT THE PROC CAN PULL TOP 20 OF EACH BY VARIOUS FACTORS
6.  TO HANDLE QUERY PLANS AS ELEGANTLY AS POSSIBLE.  PULLING QUERY PLANS IS PARAMETERIZED, AND ONLY 1 PLAN PER QUERY HASH IS INCLUDED.
9.  TO ISOLATE FUNCTIONS

/NEXT-GEN GOALS INCLUDE
7.  TO PARSE THE QUERY PLANS ENOUGH TO FLAG THOSE WITH MISSING INDICES, INDEX SCANS, AND IMPLICIT CONVERSIONS, ETC.
8.  TO CAPTURE 2 POINTS IN TIME_MS SO THAT TOTALS CAN BE NORMALIZED.  IN THE SHORT TERM, CONSIDER AVERAGING BY TIME, IN ADDITION TO BY EXECUTION COUNT.
10. TO GET TO WORK FOR CSV LIST OF PROCS WITHOUT REQUIRING FUNCTION.
*/

/* SEE SET_OPTIONS DESCRIPTIONS  AT HTTP://MSDN.MICROSOFT.COM/EN-US/LIBRARY/MS189472.ASPX*/

SET NOCOUNT ON

--SET STATISTICS IO ON
--SET STATISTICS TIME_MS ON
--SET STATISTICS IO OFF
--SET STATISTICS TIME_MS OFF
------ dbcc freeproccache
---------------------------------------------------------------------------------------------------------------
BEGIN /*VARIABLE DECLARATION*/
---------------------------------------------------------------------------------------------------------------
/* SELECT OUTPUT TYPES.  CAN MULTI-SELECT*/
DECLARE @RETURN_PLAN_CACHE_SUMMARY       TINYINT = 0
DECLARE @SHOW_OBJECT_STATS               TINYINT = 1
DECLARE @SHOW_OBJECT_STATEMENT_DETAILS   TINYINT = 1
DECLARE @SHOW_AD_HOC                     TINYINT = 1
--DECLARE @SHOW_QUERY_PLAN_ATTRIBUTES      TINYINT = 0 -- MULTIPLE ROWS PER OBJECT.  MAY BE HARD TO WORK WITH FOR LARGE RESULT SETS.


/*CAUTION:  RETURNING QUERY PLANS FOR ALL PROCEDURES CAN BE VERY RESOURCE_INTENSIVE.  
USE CAREFULLY.  FOR EXAMPLE, USE WHEN OUTPUT HAS BEEN FILTERED IN SOME SUBSTANTIAL WAY, 
                              AND AFTER KNOWING HOW MANY RESULTS THE FILTER RETURNS.
*/
DECLARE @INCLUDE_OBJECT_QUERY_PLANS                  BIT = 0
DECLARE @INCLUDE_AD_HOC_QUERY_PLANS                  BIT = 0
DECLARE @RETURN_PLANS_AS_XML_OR_TEXT        NVARCHAR(10) = N'xml' -- OPTIONS ARE 'XML' AND 'TEXT'  LARGE PLANS DON'T RETURN XML TO SSMS WINDOW, WHICH HAS AN XML SIZE LIMIT.  
                                                                  -- IF PLAN IS BLANK, TRY THE 'TEXT' OPTION.

---------------------------------------------------------------------------------------------------------------

/* APPLY VARIOUS FILTERS FOR OBJECT-BASED OUTPUTS (MEANING PROCS AND TRIGGERS - NOT FUNCTIONS YET).

FILTER BY OBJECT NAME, BY OBJECT TYPE (PROC OR TRIGGER), OR WITH WILDCARD SEARCHES AGAINST OBJECT NAMES.
iF OBJECT NAME OR WILDCARD FILTERS ARE APPLIED, AD HOC OUTPUT IS NOT PRODUCED.  I'M ASSUMING YOU'RE INTERESTED IN 
SOMETHING SPECIFIC IN THOSE CASES.*/

DECLARE @OBJECT_NAME_LIST                        SYSNAME = N'*'  -- ENTER EITHER'*' OR  CSV LIST OF NAMES (PROCS, TRIGGERS, FUNCTIONS)
                                                                         -- IF SPECIFIC NAME IS ENTERED, THE @SHOW_AD_HOC OPTION IS SET TO 0.  
                                                                         -- IF PROC NAME IS PROVIDED, THE ASSUMPTION IS THAT YOU'RE RUNNING IN THAT PROC'S DB (MAYBE - NOT CODED)

DECLARE @OBJECT_TYPE                         NVARCHAR(20) = N'*' -- OPTIONS INCLUDE 'SQL_STORED_PROCEDURE', 'SQL_TRIGGER', 'CLR_TABLE_VALUED_FUNCTION', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_TABLE_VALUED_FUNCTION'', '*'
DECLARE @OBJECT_NAME_LIST_WILDCARD_PATTERN  NVARCHAR(100) = N''  -- '' - NO WILDCARD SEARCH.   
                                                            -- 'TEXT%' -- TEXT IS EXACT AT BEGINNING. VERY FAST'
                                                            -- '%TEXT' -- FULL-ON WILDCARD SEARCH.  VERY LIKELY TO BE VERY SLOW
                                                            -- '%TEXT%' -- FULL-ON WILDCARD SEARCH.  VERY LIKELY TO BE VERY SLOW


DECLARE @AC_HOC_TEXT                  NVARCHAR(4000) = N'' -- USE WITH CAUTION. NOTE WILDCARD OPTIONS FOR NEXT PARAMETER
DECLARE @AC_HOC_TEXT_WILDCARD_PATTERN NVARCHAR(100)  = N'' -- OPTIONS INCLUDE '' (FULL TEXT PROVDED - MATCH COMPLETELY WITH NO WILDCARDS.  FASTEST OPTION)'
                                                           --                 'TEXT%' -- TEXT IS EXACT AT BEGINNING.  ALSO VERY FAST'
                                                           --                 '%TEXT%' -- FULL-ON WILDCARD SEARCH.  VERY LIKELY TO BE VERY SLOW
                                                           -- NOT SURE HOW THIS WILL WORK WITH CRLFS, TABS, ETC.  WILL NEED TO TEST.
---------------------------------------------------------------------------------------------------------------
/* APPLY OTHER FILTERS BEYOND NAMES, HAVING TO DO WITH THE STATS OR QUERY PLANS.
NOT CODED */                      
DECLARE @ONLY_OBJECTS_WITH_MULTIPLE_PLANS            BIT = 0  --WHEN SELECTED, QUERY PLAN OPTIONS THAT CAN IMPACT PLAN GENERATION ARE INCLUDED.
DECLARE @RETURN_PLANS_WITH_MISSING_INDICES       TINYINT = 1 -- CAUTION!  iF SELECTED, QUERY PLANS WILL BE PULLED.  THIS WILL BE EXPENSIVE FOR LARGE RECORD SETS.  IT'S PARSING THE XML.
DECLARE @RETURN_PLANS_WITH_index_scans           TINYINT = 1 -- CAUTION!  iF SELECTED, QUERY PLANS WILL BE PULLED.  THIS WILL BE EXPENSIVE FOR LARGE RECORD SETS.  IT'S PARSING THE XML.
DECLARE @RETURN_PLANS_WITH_implicit_conversions  TINYINT = 1 -- CAUTION!  iF SELECTED, QUERY PLANS WILL BE PULLED.  THIS WILL BE EXPENSIVE FOR LARGE RECORD SETS.  IT'S PARSING THE XML.

---------------------------------------------------------------------------------------------------------------
/* LOOK FOR WORST OFFENDERS FOR CPU, PHYSICAL I/O, ETC.  
   EXECUTION COUNT IS USEFUL BECAUSE IT HELPS YOU WEED OUT FALSE POSITIVES.  IF SOMETHING IS THE #3 TOTAL CPU CONSUMER BUT #1 IN EXECUTION COUNT, IT'S PROBABLY A FALSE POSITIVE.
   OF COURSE, THAT'S REFLECTED IN THE AVG CPU RANKING, BUT STILL!

   BECAUSE PLANS CAN STAY IN CACHE FOR VARYING LENGTHS OF TIME, LOOKING AT TOTAL COUNTS CAN BE MISLEADING.  
*/

/* QUICK WAYS TO AVOID HAVING TO SET ALL THE INDIVIDUAL RANKING FILTERS.  "NO" TOP FILTERS WINS OVER "ALL".  */
DECLARE @APPLY_ALL_TOP_FILTERS TINYINT = 1
DECLARE @APPLY_NO_TOP_FILTERS  TINYINT = 1
---------------------------------------------------------
DECLARE @TOP_N_VALUE                            INT = 30

DECLARE @RETURN_TOP_TOTAL_EXECUTION_COUNT   TINYINT = 1
                                            
DECLARE @RETURN_TOP_AVG_EXECUTION_TIME_MS      TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_EXECUTION_TIME_MS    TINYINT = 0
DECLARE @RETURN_MAX_EXECUTION_TIME_MS          TINYINT = 0
DECLARE @RETURN_MIN_EXECUTION_TIME_MS          TINYINT = 0
                                            
DECLARE @RETURN_TOP_AVG_CPU                 TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_CPU               TINYINT = 0
DECLARE @RETURN_MAX_CPU                     TINYINT = 0
DECLARE @RETURN_MIN_CPU                     TINYINT = 0
                                            
DECLARE @RETURN_TOP_AVG_PHYSICAL_READS      TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_PHYSICAL_READS    TINYINT = 0
DECLARE @RETURN_MAX_PHYSICAL_READS          TINYINT = 0
DECLARE @RETURN_MIN_PHYSICAL_READS          TINYINT = 0
                                            
DECLARE @RETURN_TOP_AVG_LOGICAL_READS       TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_LOGICAL_READS     TINYINT = 0
DECLARE @RETURN_MAX_LOGICAL_READS           TINYINT = 0
DECLARE @RETURN_MIN_LOGICAL_READS           TINYINT = 0
                                            
DECLARE @RETURN_TOP_AVG_LOGICAL_WRITES      TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_LOGICAL_WRITES    TINYINT = 0
DECLARE @RETURN_MAX_LOGICAL_WRITES          TINYINT = 0
DECLARE @RETURN_MIN_LOGICAL_WRITES          TINYINT = 0
                                         

/* AD HOC RANKING ONLY*/         
DECLARE @RETURN_TOP_CACHED_ENTRIES          TINYINT = 0                     
DECLARE @RETURN_TOP_AVG_ROWS                TINYINT = 0
DECLARE @RETURN_TOP_TOTAL_ROWS              TINYINT = 0
DECLARE @RETURN_MAX_ROWS                    TINYINT = 0
DECLARE @RETURN_MIN_ROWS                    TINYINT = 0


END /*VARIABLE DECLARATION*/
-----------------------------------------------------------------------------------------------------------
BEGIN /* VARIABLE CLEAN UP.  
      E.G., WON'T SHOW OBJECT STATEMENT DETAILS WITHOUT ALSO SHOWING OBJECT.
            IF AD HOC SELECTED, HAVE TO DO BASIC OBJECT STATEMENT DETAILS TO FIGURE OUT WHICH QUERY STATS AREN'T AD HOC, BUT NEED TO SUPPRESS OUTPUTS IF NOT 
               SELECTED BY USER.
     */
-----------------------------------------------------------------------------------------------------------
   IF @SHOW_OBJECT_STATEMENT_DETAILS = 1
      BEGIN
         SET @SHOW_OBJECT_STATS= 1
      END

IF @OBJECT_NAME_LIST_WILDCARD_PATTERN = '*' 
   BEGIN
      SET @OBJECT_NAME_LIST_WILDCARD_PATTERN = ''
   END

IF  @AC_HOC_TEXT_WILDCARD_PATTERN = '*' 
   BEGIN
      SET @AC_HOC_TEXT_WILDCARD_PATTERN = ''
   END

   /* if running for specific objects, don't return ad hoc*/
   IF @OBJECT_NAME_LIST <> '*' OR @OBJECT_NAME_LIST_WILDCARD_PATTERN <> ''
      BEGIN
         SET @SHOW_AD_HOC = 0
      END

   IF @SHOW_OBJECT_STATS = 0
      BEGIN
         SET @INCLUDE_OBJECT_QUERY_PLANS = 0
         SET @ONLY_OBJECTS_WITH_MULTIPLE_PLANS = 0
      END

   IF @OBJECT_NAME_LIST <> '*' 
      BEGIN
         SET  @OBJECT_NAME_LIST_WILDCARD_PATTERN = '' -- NO WILDCARD SEARCH. 
      END

------------------------------------------------------------------------------------
   DECLARE @TOP_N_OBJECT_RANKINGS_CHECKED      INT = 0
   --DECLARE @TOP_N_STATEMENT_RANKINGS_CHECKED   INT = 0


   IF @APPLY_ALL_TOP_FILTERS = 1
      BEGIN                                      
         SET @RETURN_TOP_TOTAL_EXECUTION_COUNT   = 1
         SET @RETURN_TOP_AVG_EXECUTION_TIME_MS      = 1
         SET @RETURN_TOP_TOTAL_EXECUTION_TIME_MS    = 1
         SET @RETURN_MAX_EXECUTION_TIME_MS          = 1
         SET @RETURN_MIN_EXECUTION_TIME_MS          = 1
                                                 
         SET @RETURN_TOP_AVG_CPU                 = 1
         SET @RETURN_TOP_TOTAL_CPU               = 1
         SET @RETURN_MAX_CPU                     = 1
         SET @RETURN_MIN_CPU                     = 1
                                                 
         SET @RETURN_TOP_AVG_PHYSICAL_READS      = 1
         SET @RETURN_TOP_TOTAL_PHYSICAL_READS    = 1
         SET @RETURN_MAX_PHYSICAL_READS          = 1
         SET @RETURN_MIN_PHYSICAL_READS          = 1
                                                 
         SET @RETURN_TOP_AVG_LOGICAL_READS       = 1
         SET @RETURN_TOP_TOTAL_LOGICAL_READS     = 1
         SET @RETURN_MAX_LOGICAL_READS           = 1
         SET @RETURN_MIN_LOGICAL_READS           = 1
                                                 
         SET @RETURN_TOP_AVG_LOGICAL_WRITES      = 1
         SET @RETURN_TOP_TOTAL_LOGICAL_WRITES    = 1
         SET @RETURN_MAX_LOGICAL_WRITES          = 1
         SET @RETURN_MIN_LOGICAL_WRITES          = 1

         SET @TOP_N_OBJECT_RANKINGS_CHECKED = 
                 @RETURN_TOP_TOTAL_EXECUTION_COUNT         
               + @RETURN_TOP_AVG_EXECUTION_TIME_MS   + @RETURN_TOP_TOTAL_EXECUTION_TIME_MS + @RETURN_MAX_EXECUTION_TIME_MS + @RETURN_MIN_EXECUTION_TIME_MS                                                          
               + @RETURN_TOP_AVG_CPU              + @RETURN_TOP_TOTAL_CPU            + @RETURN_MAX_CPU            + @RETURN_MIN_CPU                                                                     
               + @RETURN_TOP_AVG_PHYSICAL_READS   + @RETURN_TOP_TOTAL_PHYSICAL_READS + @RETURN_MAX_PHYSICAL_READS + @RETURN_MIN_PHYSICAL_READS                                                          
               + @RETURN_TOP_AVG_LOGICAL_READS    + @RETURN_TOP_TOTAL_LOGICAL_READS  + @RETURN_MAX_LOGICAL_READS  + @RETURN_MIN_LOGICAL_READS                  
               + @RETURN_TOP_AVG_LOGICAL_WRITES   + @RETURN_TOP_TOTAL_LOGICAL_WRITES + @RETURN_MAX_LOGICAL_WRITES + @RETURN_MIN_LOGICAL_WRITES 

         SET @RETURN_TOP_CACHED_ENTRIES          = 1
         SET @RETURN_TOP_AVG_ROWS                = 1
         SET @RETURN_TOP_TOTAL_ROWS              = 1
         SET @RETURN_MAX_ROWS                    = 1
         SET @RETURN_MIN_ROWS                    = 1
      END --IF @APPLY_ALL_TOP_FILTERS = 1

   IF @APPLY_NO_TOP_FILTERS = 1
      BEGIN
         SET @RETURN_TOP_TOTAL_EXECUTION_COUNT    = 0
         SET @RETURN_TOP_AVG_EXECUTION_TIME_MS       = 0
         SET @RETURN_TOP_TOTAL_EXECUTION_TIME_MS     = 0
         SET @RETURN_MAX_EXECUTION_TIME_MS           = 0
         SET @RETURN_MIN_EXECUTION_TIME_MS           = 0
                                                       
         SET @RETURN_TOP_AVG_CPU                  = 0
         SET @RETURN_TOP_TOTAL_CPU                = 0
         SET @RETURN_MAX_CPU                      = 0
         SET @RETURN_MIN_CPU                      = 0
                                                       
         SET @RETURN_TOP_AVG_PHYSICAL_READS       = 0
         SET @RETURN_TOP_TOTAL_PHYSICAL_READS     = 0
         SET @RETURN_MAX_PHYSICAL_READS           = 0
         SET @RETURN_MIN_PHYSICAL_READS           = 0
                                                        
         SET @RETURN_TOP_AVG_LOGICAL_READS        = 0
         SET @RETURN_TOP_TOTAL_LOGICAL_READS      = 0
         SET @RETURN_MAX_LOGICAL_READS            = 0
         SET @RETURN_MIN_LOGICAL_READS            = 0
                                                        
         SET @RETURN_TOP_AVG_LOGICAL_WRITES       = 0
         SET @RETURN_TOP_TOTAL_LOGICAL_WRITES     = 0
         SET @RETURN_MAX_LOGICAL_WRITES           = 0
         SET @RETURN_MIN_LOGICAL_WRITES           = 0

         SET @TOP_N_OBJECT_RANKINGS_CHECKED = 21 -- ALL OBJECT-LEVEL RANKINGS

         SET @RETURN_TOP_CACHED_ENTRIES          = 0
         SET @RETURN_TOP_AVG_ROWS                = 0
         SET @RETURN_TOP_TOTAL_ROWS              = 0
         SET @RETURN_MAX_ROWS                    = 0
         SET @RETURN_MIN_ROWS                    = 0

      END --IF @APPLY_NO_TOP_FILTERS = 1

         IF @TOP_N_OBJECT_RANKINGS_CHECKED = 0    SET @TOP_N_OBJECT_RANKINGS_CHECKED = 21 -- ALL OBJECT-LEVEL RANKINGS


END /* VARIABLE CLEAN UP.*/
-----------------------------------------------------------------------------------------------------------
BEGIN /* DROP TEMP TABLES */
-----------------------------------------------------------------------------------------------------------

   IF OBJECT_ID(N'TEMPDB..#OBJECT_STATS') > 0 DROP TABLE #OBJECT_STATS

END /* DROP TEMP TABLES */


-----------------------------------------------------------------------------------------------------------
/* OUTPUTS */
-----------------------------------------------------------------------------------------------------------

IF @RETURN_PLAN_CACHE_SUMMARY = 1
   BEGIN
      SELECT '@RETURN_PLAN_CACHE_SUMMARY' AS OUTPUT_TYPE
         , RP.name AS RESOURCE_POOL
         , cacheobjtype AS CACHE_TYPE
         , OBJTYPE AS OBJECT_TYPE
         , CASE WHEN USECOUNTS = 1 THEN 'SINGLE USE' ELSE 'MULTI USE' END AS PLAN_REUSE
         , SUM(CAST(USECOUNTS AS bigint)) AS USE_COUNT
         , COUNT_BIG(*) AS PLAN_COUNT
         , CAST(SUM(CAST(USECOUNTS AS bigint)) * 1.0/COUNT_BIG(*) AS DECIMAL(38, 2)) AS AVG_USES_PER_PLAN
         , CAST(SUM(SIZE_IN_BYTES* 1.0/1048576) AS DECIMAL(38, 2)) AS TOTAL_PLAN_MB
         , CAST(SUM(SIZE_IN_BYTES* 1.0/1048576)/ COUNT_BIG(*)  AS DECIMAL(38, 2))  AS AVG_PLAN_MB
         , CAST(MIN(SIZE_IN_BYTES* 1.0/1048576) AS DECIMAL(38, 2)) AS MIN_PLAN_MB
         , CAST(MAX(SIZE_IN_BYTES* 1.0/1048576) AS DECIMAL(38, 2)) AS MAX_PLAN_MB
      FROM sys.dm_exec_cached_plans CP
      JOIN sys.resource_governor_resource_pools RP ON RP.pool_id = CP.pool_id
      WHERE cacheobjtype like 'COMPILED PLAN%'
      GROUP BY cacheobjtype 
         , OBJTYPE 
         , CASE WHEN USECOUNTS = 1 THEN 'SINGLE USE' ELSE 'MULTI USE' END
         , RP.NAME
      ORDER BY OBJECT_TYPE, PLAN_REUSE, RP.NAME
   END --IF @RETURN_PLAN_CACHE_SUMMARY = 1


------------------------------------------------------------------------------------------------------

IF @SHOW_OBJECT_STATS = 1 OR @SHOW_OBJECT_STATEMENT_DETAILS  = 1
   BEGIN 
      -------------------------------------------------------------------------------------------------
      BEGIN /* PULL OBJECTS FROM ALL DATABASES.  
      THIS IS NECESSARY BECAUSE THE PROC, TRIGGER, AND QUERY STATS DMVS WORK ACROSS DATABASES, BUT SYS.OBJECTS IS DB-SPECIFIC.
      IN SYSTEMS WITH LARGE NUMBERS OF DATABASES, THIS WOULD HAVE TO BE RUN IN EACH DB TO GET PROC NAMES.*/

         DECLARE @DEBUG_ONLY BIT = 0 -- BY DEFAULT, THE DYNAMIC SQL WILL BE PRINTED, NOT EXECUTED.
         DECLARE @ALL_DBS TINYINT = 1
         DECLARE @CURRENT_DB_ONLY BIT = 0
         DECLARE @USER_DBS_ONLY BIT = 0
         DECLARE @SYSTEM_DBS_ONLY BIT = 0
         DECLARE @DB_INCLUDE_LIST NVARCHAR(1000) = '*'--USE CSV LIST
         DECLARE @DB_EXCLUDE_LIST NVARCHAR(1000) = '*'

         IF OBJECT_ID(N'TEMPDB..#DB_LIST') IS NOT NULL DROP TABLE #DB_LIST
         IF OBJECT_ID(N'TEMPDB..#INCLUDED_DBS') IS NOT NULL DROP TABLE #INCLUDED_DBS
         IF OBJECT_ID(N'TEMPDB..#EXCLUDED_DBS') IS NOT NULL DROP TABLE #EXCLUDED_DBS

         CREATE TABLE #DB_LIST (ROW_NUM INT IDENTITY (1, 1), DATABASE_ID INT, DATABASE_NAME SYSNAME)
         CREATE TABLE #INCLUDED_DBS  ( DATABASE_NAME SYSNAME )
         CREATE TABLE #EXCLUDED_DBS ( DATABASE_NAME SYSNAME)

         /* DEAL WITH CSV LISTS*/
         SET @DB_INCLUDE_LIST = UPPER(LTRIM(RTRIM(@DB_INCLUDE_LIST)))

         IF @DB_INCLUDE_LIST IS NULL OR @DB_INCLUDE_LIST = ''  OR @DB_INCLUDE_LIST = 'ALL' OR @DB_INCLUDE_LIST = '*'  OR @DB_INCLUDE_LIST = 'NULL' 
               BEGIN  
                  SET @DB_INCLUDE_LIST = '*'  
               END 

               IF @DB_INCLUDE_LIST <>'*'
                  BEGIN
                     insert into #INCLUDED_DBS
                     SELECT LTRIM(RTRIM(item ))
                     from (
                           SELECT Item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                           FROM 
                           ( 
                             SELECT x = CONVERT(XML, '<i>' 
                               + REPLACE(@DB_INCLUDE_LIST, ',', '</i><i>') 
                               + '</i>').query('.')
                           ) AS a CROSS APPLY x.nodes('i') AS y(i) ) x
                     WHERE CHARINDEX(@DB_INCLUDE_LIST, item )<> 0 OR @DB_INCLUDE_LIST = '*'
                  END

         SET @DB_EXCLUDE_LIST = UPPER(LTRIM(RTRIM(@DB_EXCLUDE_LIST)))

         IF @DB_EXCLUDE_LIST IS NULL OR @DB_EXCLUDE_LIST = ''  OR @DB_EXCLUDE_LIST = 'ALL' OR @DB_EXCLUDE_LIST = '*'  OR @DB_EXCLUDE_LIST = 'NULL' 
               BEGIN  
                  SET @DB_EXCLUDE_LIST = '*'  
               END 

               IF @DB_EXCLUDE_LIST <>'*'
                  BEGIN
                     insert into #EXCLUDED_DBS
                     SELECT LTRIM(RTRIM(item ))
                     from (
                           SELECT Item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                           FROM 
                           ( 
                             SELECT x = CONVERT(XML, '<i>' 
                               + REPLACE(@DB_EXCLUDE_LIST, ',', '</i><i>') 
                               + '</i>').query('.')
                           ) AS a CROSS APPLY x.nodes('i') AS y(i) ) x
                     WHERE CHARINDEX(@DB_EXCLUDE_LIST, item )<> 0 OR @DB_EXCLUDE_LIST = '*'
                  END


         INSERT INTO #DB_LIST (DATABASE_ID, DATABASE_NAME)
            SELECT DB.DATABASE_ID, DB.NAME 
            FROM SYS.DATABASES DB
            WHERE STATE_DESC = 'ONLINE'
            AND (@ALL_DBS = 1)
            AND (@CURRENT_DB_ONLY = 1 AND NAME = DB_NAME() OR @CURRENT_DB_ONLY = 0)
            AND ((@USER_DBS_ONLY = 1 AND NAME NOT IN ('MASTER', 'MODEL', 'MSDB', 'TEMPDB', 'DISTRIBUTION')) OR @USER_DBS_ONLY = 0)
            AND ((@SYSTEM_DBS_ONLY = 1 AND NAME  IN ('MASTER', 'MODEL', 'MSDB', 'DISTRIBUTION')) OR @SYSTEM_DBS_ONLY = 0)
            AND ((@DB_INCLUDE_LIST <> '*' AND NAME IN (SELECT DATABASE_NAME FROM #INCLUDED_DBS)) OR @DB_INCLUDE_LIST = '*')
            AND ((@DB_EXCLUDE_LIST <> '*' AND NAME NOT IN (SELECT DATABASE_NAME FROM #EXCLUDED_DBS)) OR @DB_EXCLUDE_LIST = '*')
--and DB.name not like '%(%'
--and DB.name not like '%-%'
--and DB.name not like '%!%'
--and DB.name not like '%&%'

         DECLARE @COUNTER INT = 1
         DECLARE @MAX_COUNTER INT = (SELECT MAX(ROW_NUM) FROM #DB_LIST)
         DECLARE @SQL_TEXT NVARCHAR(4000) = ''
         DECLARE @DATABASE_NAME SYSNAME
         DECLARE @DATABASE_ID INT

         IF OBJECT_ID(N'TEMPDB..#OBJECTS') IS NOT NULL DROP TABLE #OBJECTS  

         CREATE TABLE #OBJECTS (DATABASE_ID INT, DATABASE_NAME SYSNAME, OBJECT_ID INT, NAME SYSNAME, OBJECT_TYPE SYSNAME)


         WHILE @COUNTER <= @MAX_COUNTER
            BEGIN
               SELECT @DATABASE_NAME = DATABASE_NAME, @DATABASE_ID = DATABASE_ID FROM #DB_LIST WHERE ROW_NUM = @COUNTER

               SET @SQL_TEXT = N'
               PRINT ''--  STARTING @DATABASE_NAME, @COUNTER OF @MAX_COUNTER   --''
               USE [@DATABASE_NAME]
                 INSERT INTO #OBJECTS
                  SELECT @DATABASE_ID, ''@DATABASE_NAME'',  O.OBJECT_ID, O.NAME, TYPE_DESC
                  FROM  sys.objects O
                  WHERE TYPE_DESC <>(''SYSTEM_TABLE'') AND TYPE_DESC NOT LIKE ''%CONSTRAINT%''

                  '
               SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@DATABASE_ID', @DATABASE_ID)
               SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@DATABASE_NAME', @DATABASE_NAME)
               SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@COUNTER', @COUNTER)
               SET @SQL_TEXT = REPLACE(@SQL_TEXT, '@MAX_COUNTER', @MAX_COUNTER)

     
               IF @DEBUG_ONLY = 0 
                  BEGIN
                     EXEC  (@SQL_TEXT)
                  END

               SET @COUNTER = @COUNTER + 1

            END -- WHILE @COUNTER <= @MAX_COUNTER
      END /* PULL OBJECTS FROM ALL DATABASES.  */


         /* FIND FUNCTIONS.  NOT DIRECTLY AVAILABLE.  NEED TO GO TO ATTRIBUTES*/

         IF OBJECT_ID(N'TEMPDB..#ATTRIBUTES') IS NOT NULL DROP TABLE #ATTRIBUTES

         CREATE TABLE #ATTRIBUTES
         (
               plan_handle  varbinary(64)
             , attribute    nvarchar(128)
             , value        sql_variant
             , is_cache_key bit
         )

         insert INTO #ATTRIBUTES
            SELECT  plan_handle, Z.*
            FROM sys.dm_exec_query_stats X
            CROSS APPLY SYS.DM_EXEC_PLAN_ATTRIBUTES (PLAN_HANDLE) Z
            where attribute in ('dbid',  'objectid')

         CREATE INDEX IX1 ON #ATTRIBUTES (PLAN_HANDLE, ATTRIBUTE) INCLUDE (VALUE)

         IF OBJECT_ID(N'TEMPDB..#FUNCTIONS_prep') IS NOT NULL DROP TABLE #FUNCTIONS_prep

         CREATE TABLE #FUNCTIONS_prep
         (
               plan_handle varbinary(64)
             , database_id sql_variant
             , object_id   sql_variant
         )
  

         insert into #FUNCTIONS_prep
            select distinct plan_handle
            , (select distinct value from #ATTRIBUTES a2 where attribute = 'dbid' and a2.plan_handle = a.plan_handle)  as database_id
            , (select distinct value from #ATTRIBUTES a2 where attribute = 'objectid' and a2.plan_handle = a.plan_handle)  as object_id        
            from #ATTRIBUTES a

         IF OBJECT_ID(N'TEMPDB..#FUNCTIONS') IS NOT NULL DROP TABLE #FUNCTIONS

         CREATE TABLE #functions
         (
               plan_handle varbinary(64)
             , database_id sql_variant
             , object_id   sql_variant
             , NAME        nvarchar(128)
             , OBJECT_type nvarchar(128)
         )

         insert into #functions
         select f.*, o.NAME, o.OBJECT_type
         from #FUNCTIONS_prep f
         join #OBJECTS o on o.DATABASE_ID = f.database_id and o.OBJECT_ID = f.object_id
                  where o.object_type IN 
                     (N'CLR_TABLE_VALUED_FUNCTION', 
                     N'SQL_INLINE_TABLE_VALUED_FUNCTION', 
                     N'SQL_SCALAR_FUNCTION', 
                     N'SQL_TABLE_VALUED_FUNCTION')


      IF @SHOW_AD_HOC = 1
      -----------------------------------------------------------------------------------------------------
         /* IN ORDER TO PULL AD HOC STATEMENTS, I NEED THE PLAN HANDLES FOR ALL OBJECTS.
             HOWEVER, I DON'T WANT TO HAVE TO PULL QUERY TEXT AND PLANS FOR EVERYTHING IF I'M APPLYING OBJECT-LEVEL FILTERS
            SO I LOAD THE OBJECTS INTO A SECOND TABLE (#ALL_OBJECTS) AND THEN, IF AD HOC SELECTED, DO A PULL FROM PROC/TRIGGER STATS JUST TO GET THE PLAN HANDLES.*/

      /*NEED TO HAVE PLAN HANDLES FOR ALL OBJECTS TO EXCLUDE FROM QUERY STATS*/
         BEGIN

            IF OBJECT_ID(N'TEMPDB..#ALL_OBJECTS') is not null DROP TABLE #ALL_OBJECTS
            IF OBJECT_ID(N'TEMPDB..#ALL_OBJECT_STATS') is not null DROP TABLE #ALL_OBJECT_STATS

            CREATE TABLE #ALL_OBJECTS
            (
                  DATABASE_ID   int
                , DATABASE_NAME nvarchar(128)
                , OBJECT_ID     int
                , NAME          nvarchar(128)
                , OBJECT_TYPE   nvarchar(128)
            )
  
            insert INTO #ALL_OBJECTS
               SELECT *
               FROM #OBJECTS

            CREATE TABLE #ALL_OBJECT_STATS
            (
                  DATABASE_NAME nvarchar(128)
                , DATABASE_ID   int
                , NAME          nvarchar(128)
                , OBJECT_ID     int
                , OBJECT_TYPE   nvarchar(128)
                , PLAN_HANDLE   varbinary(64)
            )
  

            insert INTO #ALL_OBJECT_STATS
               SELECT P.DATABASE_NAME
                  , P.DATABASE_ID  
                  , P.NAME
                  , P.OBJECT_ID
                  , P.OBJECT_TYPE
                  , s.PLAN_HANDLE
               FROM (SELECT * FROM SYS.DM_EXEC_PROCEDURE_STATS
                     UNION ALL
                     SELECT * FROM SYS.DM_EXEC_TRIGGER_STATS) S
               JOIN #ALL_OBJECTS P ON P.OBJECT_ID = S.OBJECT_ID
               AND P.DATABASE_ID = S.database_id

            INSERT INTO #ALL_OBJECT_STATS
               select DATABASE_NAME
                  , O.DATABASE_ID
                  , o.NAME
                  , O.OBJECT_ID
                  , o.OBJECT_TYPE
                  , PLAN_HANDLE
                  from #FUNCTIONS  a
                  join #objects o on o.object_id = a.object_id and o.database_id = a.database_id
                  where o.object_type IN 
                     (N'CLR_TABLE_VALUED_FUNCTION', 
                     N'SQL_INLINE_TABLE_VALUED_FUNCTION', 
                     N'SQL_SCALAR_FUNCTION', 
                     N'SQL_TABLE_VALUED_FUNCTION')

         END --IF @SHOW_AD_HOC = 1

      -----------------------------------------------------------------------------------------------------

      /* APPLY OBJECT-LEVEL FILTERS */
      IF OBJECT_ID(N'TEMPDB..#OBJECT_LIST') IS NOT NULL DROP TABLE #OBJECT_LIST

      CREATE TABLE #OBJECT_LIST
      (
            ITEM nvarchar(4000)
      )

      IF @OBJECT_NAME_LIST <> '*'
         BEGIN
            insert INTO #OBJECT_LIST
               SELECT LTRIM(RTRIM(item )) AS ITEM
               from (
                     SELECT Item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                     FROM 
                     ( 
                        SELECT x = CONVERT(XML, '<i>' 
                           + REPLACE(@OBJECT_NAME_LIST, ',', '</i><i>') 
                           + '</i>').query('.')
                     ) AS a CROSS APPLY x.nodes('i') AS y(i) ) x

            DELETE FROM O
            FROM #OBJECTS O
            LEFT JOIN #OBJECT_LIST OL ON OL.ITEM = O.NAME
            WHERE OL.ITEM IS NULL

         END --IF @OBJECT_NAME_LIST <> '*'

      IF @OBJECT_TYPE <> '*'
         BEGIN
            DELETE FROM #OBJECTS WHERE OBJECT_TYPE <> @OBJECT_TYPE
         END -- IF @OBJECT_TYPE <> '*'

      IF @OBJECT_NAME_LIST_WILDCARD_PATTERN <> ''
         BEGIN
            DELETE FROM #OBJECTS WHERE NAME NOT LIKE @OBJECT_NAME_LIST_WILDCARD_PATTERN
         END


     -------------------------------------------------------------------------------
     /* LOAD OBJECT-LEVEL STATISTICS*/

      IF OBJECT_ID(N'TEMPDB..#OBJECT_STATS_PREP') IS NOT NULL DROP TABLE #OBJECT_STATS_PREP

      CREATE TABLE #OBJECT_STATS_PREP
      (  DATABASE_NAME SYSNAME
         , DATABASE_ID INT
         , OBJECT_NAME SYSNAME
         , OBJECT_ID INT
         , PLAN_INSTANCE_ID INT
         , OBJECT_TYPE nvarCHAR(60)
         , CACHED_TIME_MS DATETIME
         , LAST_EXECUTION_TIME_MS DATETIME
         , CACHE_TO_LAST_EXEC_MINUTES INT
         , EXECUTION_COUNT  BIGINT
         , AVG_TIME_ms   DECIMAL(38, 2), TOTAL_TIME_ms   DECIMAL(38, 2), MIN_TIME_ms   DECIMAL(38, 2), MAX_TIME_ms   DECIMAL(38, 2)
         , AVG_CPU_MS          DECIMAL(38, 2), TOTAL_CPU_MS          DECIMAL(38, 2), MIN_CPU               DECIMAL(38, 2), MAX_CPU   DECIMAL(38, 2)
         , AVG_LOGICAL_READS     DECIMAL(38, 2), TOTAL_LOGICAL_READS     BIGINT, MIN_LOGICAL_READS     BIGINT, MAX_LOGICAL_READS     BIGINT 
         , AVG_PHYSICAL_READS    DECIMAL(38, 2), TOTAL_PHYSICAL_READS    BIGINT, MIN_PHYSICAL_READS    BIGINT, MAX_PHYSICAL_READS    BIGINT 
         , AVG_LOGICAL_WRITES    DECIMAL(38, 2), TOTAL_LOGICAL_WRITES    BIGINT, MIN_LOGICAL_WRITES    BIGINT, MAX_LOGICAL_WRITES    BIGINT 
         , PLAN_HANDLE VARBINARY(64) 
         )

      INSERT INTO #OBJECT_STATS_PREP
         SELECT P.DATABASE_NAME
            , P.DATABASE_ID  
            , P.NAME
            , P.OBJECT_ID
            , ROW_NUMBER() OVER(PARTITION BY P.DATABASE_ID, P.OBJECT_ID ORDER BY CACHED_TIME) 
            , P.OBJECT_TYPE
            , CACHED_TIME
            , LAST_EXECUTION_TIME
            , DATEDIFF(MINUTE, CACHED_TIME, LAST_EXECUTION_TIME) AS CACHE_TO_LAST_EXEC_MINUTES
            , EXECUTION_COUNT
            , TOTAL_ELAPSED_TIME/1000.0/EXECUTION_COUNT  
            , TOTAL_ELAPSED_TIME/1000.0
            , MIN_ELAPSED_TIME/1000.0
            , MAX_ELAPSED_TIME/1000.0
            , TOTAL_WORKER_TIME/1000.0/EXECUTION_COUNT 
            , TOTAL_WORKER_TIME/1000.0
            , MIN_WORKER_TIME/1000.0
            , MAX_WORKER_TIME/1000.0
            , TOTAL_LOGICAL_READS*1.0/EXECUTION_COUNT 
            , TOTAL_LOGICAL_READS
            , MIN_LOGICAL_READS
            , MAX_LOGICAL_READS
            , TOTAL_PHYSICAL_READS*1.0/EXECUTION_COUNT 
            , TOTAL_PHYSICAL_READS
            , MIN_PHYSICAL_READS
            , MAX_PHYSICAL_READS
            , TOTAL_LOGICAL_WRITES*1.0 /EXECUTION_COUNT 
            , TOTAL_LOGICAL_WRITES
            , MIN_LOGICAL_WRITES
            , MAX_LOGICAL_WRITES
            , PLAN_HANDLE
         FROM (SELECT * FROM SYS.DM_EXEC_PROCEDURE_STATS WHERE DATABASE_ID <> 32767
               UNION  ALL
               SELECT * FROM SYS.DM_EXEC_TRIGGER_STATS WHERE DATABASE_ID <> 32767) S
         JOIN #OBJECTS P ON P.OBJECT_ID = S.OBJECT_ID
         AND P.DATABASE_ID = S.database_id

         INSERT INTO #OBJECT_STATS_PREP
            SELECT 
              P.DATABASE_NAME
            , P.DATABASE_ID  
            , P.NAME
            , P.OBJECT_ID
            , ROW_NUMBER() OVER(PARTITION BY P.DATABASE_ID, P.OBJECT_ID ORDER BY CREATION_TIME) 
            , P.OBJECT_TYPE
            , QS.CREATION_TIME
            , qs.LAST_EXECUTION_TIME
            , DATEDIFF(MINUTE, CREATION_TIME, qs.LAST_EXECUTION_TIME) AS CACHE_TO_LAST_EXEC_MINUTES
            , SUM(QS.EXECUTION_COUNT)            AS EXECUTION_COUNT
            , SUM(QS.TOTAL_ELAPSED_TIME/1000.0)  
               /SUM(QS.EXECUTION_COUNT)          AS AVG_TIME
            , SUM(QS.TOTAL_ELAPSED_TIME/1000.0 ) AS  TOTAL_TIME
            , MIN(QS.MIN_ELAPSED_TIME/1000.0 )   AS MIN_TIME
            , MAX(QS.MAX_ELAPSED_TIME/1000.0 )   AS MAX_TIME
            , SUM(QS.TOTAL_WORKER_TIME/1000.0)    
               /SUM(QS.EXECUTION_COUNT)          AS AVG_CPU_MS
            , SUM(QS.TOTAL_WORKER_TIME/1000.0)          AS TOTAL_CPU_MS
            , MIN(QS.MIN_WORKER_TIME/1000.0)            AS MIN_CPU_MS
            , MAX(QS.MAX_WORKER_TIME/1000.0)            AS MAX_CPU_MS
            , SUM(QS.TOTAL_LOGICAL_READS) *1.0   
               /SUM(QS.EXECUTION_COUNT)          AS AVG_LOGICAL_READS
            , SUM(QS.TOTAL_LOGICAL_READS )       AS TOTAL_LOGICAL_READS
            , MIN(QS.MIN_LOGICAL_READS )         AS MIN_LOGICAL_READS
            , MAX(QS.MAX_LOGICAL_READS )         AS MAX_LOGICAL_READS
            , SUM(QS.TOTAL_PHYSICAL_READS *1.0)  
               /SUM(QS.EXECUTION_COUNT)          AS AVG_PHYSICAL_READS
            , SUM(QS.TOTAL_PHYSICAL_READS )      AS TOTAL_PHYSICAL_READS
            , MIN(QS.MIN_PHYSICAL_READS )        AS MIN_PHYSICAL_READS
            , MAX(QS.MAX_PHYSICAL_READS )        AS MAX_PHYSICAL_READS
            , SUM(QS.TOTAL_LOGICAL_WRITES*1.0)   
               /SUM(QS.EXECUTION_COUNT)          AS AVG_LOGICAL_WRITES
            , SUM(QS.TOTAL_LOGICAL_WRITES )      AS TOTAL_LOGICAL_WRITES
            , MIN(QS.MIN_LOGICAL_WRITES )        AS MIN_LOGICAL_WRITES
            , MAX(QS.MAX_LOGICAL_WRITES )        AS MAX_LOGICAL_WRITES
            , MAX(f.plan_handle)                 as plan_handle
            FROM SYS.DM_EXEC_QUERY_STATS QS 
            JOIN #FUNCTIONS  F ON QS.PLAN_HANDLE = F.PLAN_HANDLE 
            JOIN #OBJECTS    P ON P.OBJECT_ID = F.OBJECT_ID
                                   AND P.DATABASE_ID = F.database_id
            left join #OBJECT_STATS_PREP osp on osp.DATABASE_ID = P.DATABASE_ID and osp.OBJECT_ID = P.OBJECT_ID
            where osp.OBJECT_ID is null
            GROUP BY               
              P.DATABASE_NAME
            , P.DATABASE_ID  
            , P.NAME
            , P.OBJECT_ID
            , P.OBJECT_TYPE
            , QS.CREATION_TIME
            , qs.LAST_EXECUTION_TIME

      IF OBJECT_ID(N'TEMPDB..#OBJECT_STATS') IS NOT NULL DROP TABLE #OBJECT_STATS

      CREATE TABLE #OBJECT_STATS 
      (  DATABASE_NAME SYSNAME
         , DATABASE_ID INT
         , OBJECT_NAME SYSNAME
         , OBJECT_ID INT
         , PLAN_INSTANCE_ID INT
         , OBJECT_TYPE nvarCHAR(60)
         , CACHED_TIME_MS DATETIME
         , LAST_EXECUTION_TIME_MS DATETIME
         , CACHE_TO_LAST_EXEC_MINUTES INT
         , EXECUTION_COUNT  BIGINT
         , AVG_TIME_MS   DECIMAL(38, 2), TOTAL_TIME_MS   DECIMAL(38, 2), MIN_TIME_MS   DECIMAL(38, 2), MAX_TIME_MS   DECIMAL(38, 2)
         , AVG_CPU_MS       DECIMAL(38, 2), TOTAL_CPU_MS       DECIMAL(38, 2), MIN_CPU            DECIMAL(38, 2), MAX_CPU            DECIMAL(38, 2)
         , AVG_LOGICAL_READS  DECIMAL(38, 2), TOTAL_LOGICAL_READS  BIGINT, MIN_LOGICAL_READS  BIGINT, MAX_LOGICAL_READS  BIGINT 
         , AVG_PHYSICAL_READS DECIMAL(38, 2), TOTAL_PHYSICAL_READS BIGINT, MIN_PHYSICAL_READS BIGINT, MAX_PHYSICAL_READS BIGINT 
         , AVG_LOGICAL_WRITES DECIMAL(38, 2), TOTAL_LOGICAL_WRITES BIGINT, MIN_LOGICAL_WRITES BIGINT, MAX_LOGICAL_WRITES BIGINT 
         , PLAN_HANDLE VARBINARY(64) 
         , EXECUTION_COUNT_RANK    INT
         , AVG_TIME_RANK   INT  , TOTAL_TIME_RANK   INT, MIN_TIME_RANK   INT, MAX_TIME_RANK   INT
         , AVG_CPU_MS_RANK       INT  , TOTAL_CPU_MS_RANK       INT, MIN_CPU_RANK            INT, MAX_CPU_RANK            INT
         , AVG_LOGICAL_READS_RANK  INT  , TOTAL_LOGICAL_READS_RANK  INT, MIN_LOGICAL_READS_RANK  INT, MAX_LOGICAL_READS_RANK  INT 
         , AVG_PHYSICAL_READS_RANK INT  , TOTAL_PHYSICAL_READS_RANK INT, MIN_PHYSICAL_READS_RANK INT, MAX_PHYSICAL_READS_RANK INT 
         , AVG_LOGICAL_WRITES_RANK INT  , TOTAL_LOGICAL_WRITES_RANK INT, MIN_LOGICAL_WRITES_RANK INT, MAX_LOGICAL_WRITES_RANK INT 
         )

         INSERT INTO #OBJECT_STATS
            SELECT OSP.*
            , RANK() OVER(ORDER BY EXECUTION_COUNT DESC)
            , RANK() OVER(ORDER BY TOTAL_TIME_MS * 1.0/EXECUTION_COUNT  DESC)
            , RANK() OVER(ORDER BY TOTAL_TIME_MS DESC)
            , RANK() OVER(ORDER BY MIN_TIME_MS DESC)
            , RANK() OVER(ORDER BY MAX_TIME_MS DESC)
            , RANK() OVER(ORDER BY TOTAL_CPU_MS * 1.0 / EXECUTION_COUNT  DESC)
            , RANK() OVER(ORDER BY TOTAL_CPU_MS DESC)
            , RANK() OVER(ORDER BY MIN_CPU DESC)
            , RANK() OVER(ORDER BY MAX_CPU DESC)
            , RANK() OVER(ORDER BY TOTAL_LOGICAL_READS*1.0/EXECUTION_COUNT  DESC)
            , RANK() OVER(ORDER BY TOTAL_LOGICAL_READS DESC)
            , RANK() OVER(ORDER BY MIN_LOGICAL_READS DESC)
            , RANK() OVER(ORDER BY MAX_LOGICAL_READS DESC)
            , RANK() OVER(ORDER BY TOTAL_PHYSICAL_READS*1.0/EXECUTION_COUNT  DESC)
            , RANK() OVER(ORDER BY TOTAL_PHYSICAL_READS DESC)
            , RANK() OVER(ORDER BY MIN_PHYSICAL_READS DESC)
            , RANK() OVER(ORDER BY MAX_PHYSICAL_READS DESC)
            , RANK() OVER(ORDER BY TOTAL_LOGICAL_WRITES*1.0 /EXECUTION_COUNT  DESC)
            , RANK() OVER(ORDER BY TOTAL_LOGICAL_WRITES DESC)
            , RANK() OVER(ORDER BY MIN_LOGICAL_WRITES DESC)
            , RANK() OVER(ORDER BY MAX_LOGICAL_WRITES DESC)
          FROM #OBJECT_STATS_PREP OSP


      /* ASSIGN OVERALL RANKING, AND INCLUDE Y/N BIT VALUE, BASED ON SELECTED CRITERIA*/
      IF OBJECT_ID(N'TEMPDB..#OBJECT_STATS2') is not null DROP TABLE #OBJECT_STATS2
      SELECT *
            ,  CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_COUNT        = 1 THEN EXECUTION_COUNT_RANK      ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_EXECUTION_TIME_MS           = 1 THEN AVG_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_TIME_MS         = 1 THEN TOTAL_TIME_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_EXECUTION_TIME_MS               = 1 THEN MAX_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_MAX_EXECUTION_TIME_MS               = 1 THEN MIN_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_CPU                      = 1 THEN AVG_CPU_MS_RANK         ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_CPU                    = 1 THEN TOTAL_CPU_MS_RANK       ELSE 0 END
             + CASE WHEN @RETURN_MAX_CPU                          = 1 THEN MAX_CPU_RANK              ELSE 0 END
             + CASE WHEN @RETURN_MIN_CPU                          = 1 THEN MIN_CPU_RANK              ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_PHYSICAL_READS           = 1 THEN AVG_LOGICAL_READS_RANK    ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_PHYSICAL_READS         = 1 THEN TOTAL_LOGICAL_READS_RANK  ELSE 0 END
             + CASE WHEN @RETURN_MAX_PHYSICAL_READS               = 1 THEN MAX_LOGICAL_READS_RANK    ELSE 0 END
             + CASE WHEN @RETURN_MIN_PHYSICAL_READS               = 1 THEN MIN_LOGICAL_READS_RANK    ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_READS            = 1 THEN AVG_PHYSICAL_READS_RANK   ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_READS          = 1 THEN TOTAL_PHYSICAL_READS_RANK ELSE 0 END
             + CASE WHEN @RETURN_MAX_LOGICAL_READS                = 1 THEN MAX_PHYSICAL_READS_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_LOGICAL_READS                = 1 THEN MIN_PHYSICAL_READS_RANK   ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_WRITES           = 1 THEN AVG_LOGICAL_WRITES_RANK   ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_WRITES         = 1 THEN TOTAL_LOGICAL_WRITES_RANK ELSE 0 END
             + CASE WHEN @RETURN_MAX_LOGICAL_WRITES               = 1 THEN MAX_LOGICAL_WRITES_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_LOGICAL_WRITES               = 1 THEN MIN_LOGICAL_WRITES_RANK   ELSE 0 END AS OVERALL_SCORE 

            ,  CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_COUNT        = 1 AND EXECUTION_COUNT_RANK      <= @TOP_N_VALUE THEN 1 ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_EXECUTION_TIME_MS           = 1 AND AVG_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_TIME_MS         = 1 AND TOTAL_TIME_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_EXECUTION_TIME_MS               = 1 AND MAX_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_EXECUTION_TIME_MS               = 1 AND MIN_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_CPU                      = 1 AND AVG_CPU_MS_RANK         <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_CPU                    = 1 AND TOTAL_CPU_MS_RANK       <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_CPU                          = 1 AND MAX_CPU_RANK              <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_CPU                          = 1 AND MIN_CPU_RANK              <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_PHYSICAL_READS           = 1 AND AVG_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_PHYSICAL_READS         = 1 AND TOTAL_LOGICAL_READS_RANK  <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_PHYSICAL_READS               = 1 AND MAX_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_PHYSICAL_READS               = 1 AND MIN_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_READS            = 1 AND AVG_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_READS          = 1 AND TOTAL_PHYSICAL_READS_RANK <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_LOGICAL_READS                = 1 AND MAX_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_LOGICAL_READS                = 1 AND MIN_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_WRITES           = 1 AND AVG_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_WRITES         = 1 AND TOTAL_LOGICAL_WRITES_RANK <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_LOGICAL_WRITES               = 1 AND MAX_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_LOGICAL_WRITES               = 1 AND MIN_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END AS TOP_N_RANKING_COUNT 
      INTO #OBJECT_STATS2
      FROM #OBJECT_STATS

      /* APPLY TOP N FILTERS.  CAN'T DO THIS UNTIL I HAVE STATEMENT-LEVEL VALUES.*/
      IF @APPLY_NO_TOP_FILTERS  = 0
         BEGIN
            DELETE FROM OS2
            FROM #OBJECT_STATS2 OS2
            WHERE 
               (    (@RETURN_TOP_TOTAL_EXECUTION_COUNT  = 1 OR EXECUTION_COUNT_RANK      > @TOP_N_VALUE) 
                 AND (@RETURN_TOP_AVG_EXECUTION_TIME_MS    = 1 OR AVG_TIME_RANK     > @TOP_N_VALUE) 
                 AND (@RETURN_TOP_TOTAL_EXECUTION_TIME_MS  = 1 OR TOTAL_TIME_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_MIN_EXECUTION_TIME_MS        = 1 OR MAX_TIME_RANK     > @TOP_N_VALUE)
                 AND (@RETURN_MAX_EXECUTION_TIME_MS        = 1 OR MIN_TIME_RANK     > @TOP_N_VALUE)
                 AND (@RETURN_TOP_AVG_CPU               = 1 OR AVG_CPU_MS_RANK         > @TOP_N_VALUE)
                 AND (@RETURN_TOP_TOTAL_CPU             = 1 OR TOTAL_CPU_MS_RANK       > @TOP_N_VALUE)
                 AND (@RETURN_MAX_CPU                   = 1 OR MAX_CPU_RANK              > @TOP_N_VALUE)
                 AND (@RETURN_MIN_CPU                   = 1 OR MIN_CPU_RANK              > @TOP_N_VALUE)
                 AND (@RETURN_TOP_AVG_PHYSICAL_READS    = 1 OR AVG_LOGICAL_READS_RANK    > @TOP_N_VALUE)
                 AND (@RETURN_TOP_TOTAL_PHYSICAL_READS  = 1 OR TOTAL_LOGICAL_READS_RANK  > @TOP_N_VALUE)
                 AND (@RETURN_MAX_PHYSICAL_READS        = 1 OR MAX_LOGICAL_READS_RANK    > @TOP_N_VALUE)
                 AND (@RETURN_MIN_PHYSICAL_READS        = 1 OR MIN_LOGICAL_READS_RANK    > @TOP_N_VALUE)
                 AND (@RETURN_TOP_AVG_LOGICAL_READS     = 1 OR AVG_PHYSICAL_READS_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_TOP_TOTAL_LOGICAL_READS   = 1 OR TOTAL_PHYSICAL_READS_RANK > @TOP_N_VALUE)
                 AND (@RETURN_MAX_LOGICAL_READS         = 1 OR MAX_PHYSICAL_READS_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_MIN_LOGICAL_READS         = 1 OR MIN_PHYSICAL_READS_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_TOP_AVG_LOGICAL_WRITES    = 1 OR AVG_LOGICAL_WRITES_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_TOP_TOTAL_LOGICAL_WRITES  = 1 OR TOTAL_LOGICAL_WRITES_RANK > @TOP_N_VALUE)
                 AND (@RETURN_MAX_LOGICAL_WRITES        = 1 OR MAX_LOGICAL_WRITES_RANK   > @TOP_N_VALUE)
                 AND (@RETURN_MIN_LOGICAL_WRITES        = 1 OR MIN_LOGICAL_WRITES_RANK   > @TOP_N_VALUE))
         END  --IF @APPLY_NO_TOP_FILTERS  = 0

      if @INCLUDE_OBJECT_QUERY_PLANS = 1 AND @RETURN_PLANS_AS_XML_OR_TEXT = N'XML'
         BEGIN

IF OBJECT_ID(N'TEMPDB..plan_cache_objects_with_xml') IS NOT NULL DROP TABLE plan_cache_objects_with_xml
      CREATE TABLE plan_cache_objects_with_xml
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , OUTPUT_TYPE                varchar(36)
          , DATABASE_NAME              nvarchar(128)
          , DATABASE_ID                int
          , OBJECT_NAME                nvarchar(128)
          , OBJECT_ID                  int
          , PLAN_INSTANCE_ID           int
          , OBJECT_TYPE                nvarchar(60)
          , CACHED_TIME_MS             datetime
          , LAST_EXECUTION_TIME_MS     datetime
          , CACHE_TO_LAST_EXEC_MINUTES int
          , EXECUTION_COUNT            bigint
          , AVG_TIME_MS                decimal(38,2)
          , TOTAL_TIME_MS              decimal(38,2)
          , MIN_TIME_MS                decimal(38,2)
          , MAX_TIME_MS                decimal(38,2)
          , AVG_CPU_MS                 decimal(38,2)
          , TOTAL_CPU_MS               decimal(38,2)
          , MIN_CPU                    decimal(38,2)
          , MAX_CPU                    decimal(38,2)
          , AVG_LOGICAL_READS          decimal(38,2)
          , TOTAL_LOGICAL_READS        bigint
          , MIN_LOGICAL_READS          bigint
          , MAX_LOGICAL_READS          bigint
          , AVG_PHYSICAL_READS         decimal(38,2)
          , TOTAL_PHYSICAL_READS       bigint
          , MIN_PHYSICAL_READS         bigint
          , MAX_PHYSICAL_READS         bigint
          , AVG_LOGICAL_WRITES         decimal(38,2)
          , TOTAL_LOGICAL_WRITES       bigint
          , MIN_LOGICAL_WRITES         bigint
          , MAX_LOGICAL_WRITES         bigint
          , PLAN_HANDLE                varbinary(64)
          , OVERALL_SCORE              int
          , OVERALL_ALL_SCORE_RANKING  bigint
          , TOP_N_RANKING_COUNT        int
          , QUERY_PLAN                 xml
      )
INSERT INTO plan_cache_objects_with_xml  
            SELECT @data_set, @run_type, @run_nbr
             , 'OBJECT-LEVEL OUTPUT - XML QUERY PLAN' AS OUTPUT_TYPE
             , X.DATABASE_NAME 
             , X.DATABASE_ID 
             , X.OBJECT_NAME 
             , X.OBJECT_ID 
             , X.PLAN_INSTANCE_ID
             , X.OBJECT_TYPE 
             , X.CACHED_TIME_MS 
             , X.LAST_EXECUTION_TIME_MS 
             , X.CACHE_TO_LAST_EXEC_MINUTES 
             , EXECUTION_COUNT  
             , AVG_TIME_MS   , TOTAL_TIME_MS   , MIN_TIME_MS   , MAX_TIME_MS   
             , AVG_CPU_MS       , TOTAL_CPU_MS       , MIN_CPU            , MAX_CPU            
             , AVG_LOGICAL_READS  , TOTAL_LOGICAL_READS  , MIN_LOGICAL_READS  , MAX_LOGICAL_READS   
             , AVG_PHYSICAL_READS , TOTAL_PHYSICAL_READS , MIN_PHYSICAL_READS , MAX_PHYSICAL_READS  
             , AVG_LOGICAL_WRITES , TOTAL_LOGICAL_WRITES , MIN_LOGICAL_WRITES , MAX_LOGICAL_WRITES  
             , PLAN_HANDLE
             , OVERALL_SCORE
             , DENSE_RANK() over (ORDER BY OVERALL_SCORE) AS OVERALL_ALL_SCORE_RANKING
             , TOP_N_RANKING_COUNT
             --, case when TOP_N_RANKING_COUNT = 0 then 0 else CAST(OVERALL_SCORE * 100.0/TOP_N_RANKING_COUNT AS DECIMAL(12, 2))end AS RANKINGS_MET_PCT
             --, EXECUTION_COUNT_RANK    
             --, AVG_TIME_RANK   , TOTAL_TIME_RANK   , MIN_TIME_RANK   , MAX_TIME_RANK   
             --, AVG_CPU_MS_RANK       , TOTAL_CPU_MS_RANK       , MIN_CPU_RANK            , MAX_CPU_RANK            
             --, AVG_LOGICAL_READS_RANK  , TOTAL_LOGICAL_READS_RANK  , MIN_LOGICAL_READS_RANK  , MAX_LOGICAL_READS_RANK   
             --, AVG_PHYSICAL_READS_RANK , TOTAL_PHYSICAL_READS_RANK , MIN_PHYSICAL_READS_RANK , MAX_PHYSICAL_READS_RANK  
             --, AVG_LOGICAL_WRITES_RANK , TOTAL_LOGICAL_WRITES_RANK , MIN_LOGICAL_WRITES_RANK , MAX_LOGICAL_WRITES_RANK  
             --, STATEMENT_LEVEL_OVERALL_SCORE
             , Y.QUERY_PLAN
            FROM #OBJECT_STATS2 X
            outer APPLY SYS.DM_EXEC_QUERY_PLAN(PLAN_HANDLE) Y
            WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
            ORDER BY DATABASE_NAME, X.OBJECT_NAME, PLAN_HANDLE
         END --if @INCLUDE_OBJECT_QUERY_PLANS = 1 AND @RETURN_PLANS_AS_XML_OR_TEXT = N'XML'

      if @INCLUDE_OBJECT_QUERY_PLANS = 1 AND @RETURN_PLANS_AS_XML_OR_TEXT = N'TEXT'
         BEGIN
IF OBJECT_ID(N'TEMPDB..plan_cache_objects_with_text') IS NOT NULL DROP TABLE plan_cache_objects_with_text
      CREATE TABLE plan_cache_objects_with_text
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , OUTPUT_TYPE                varchar(36)
          , DATABASE_NAME              nvarchar(128)
          , DATABASE_ID                int
          , OBJECT_NAME                nvarchar(128)
          , OBJECT_ID                  int
          , PLAN_INSTANCE_ID           int
          , OBJECT_TYPE                nvarchar(60)
          , CACHED_TIME_MS             datetime
          , LAST_EXECUTION_TIME_MS     datetime
          , CACHE_TO_LAST_EXEC_MINUTES int
          , EXECUTION_COUNT            bigint
          , AVG_TIME_MS                decimal(38,2)
          , TOTAL_TIME_MS              decimal(38,2)
          , MIN_TIME_MS                decimal(38,2)
          , MAX_TIME_MS                decimal(38,2)
          , AVG_CPU_MS                 decimal(38,2)
          , TOTAL_CPU_MS               decimal(38,2)
          , MIN_CPU                    decimal(38,2)
          , MAX_CPU                    decimal(38,2)
          , AVG_LOGICAL_READS          decimal(38,2)
          , TOTAL_LOGICAL_READS        bigint
          , MIN_LOGICAL_READS          bigint
          , MAX_LOGICAL_READS          bigint
          , AVG_PHYSICAL_READS         decimal(38,2)
          , TOTAL_PHYSICAL_READS       bigint
          , MIN_PHYSICAL_READS         bigint
          , MAX_PHYSICAL_READS         bigint
          , AVG_LOGICAL_WRITES         decimal(38,2)
          , TOTAL_LOGICAL_WRITES       bigint
          , MIN_LOGICAL_WRITES         bigint
          , MAX_LOGICAL_WRITES         bigint
          , PLAN_HANDLE                varbinary(64)
          , OVERALL_SCORE              int
          , OVERALL_ALL_SCORE_RANKING  bigint
          , TOP_N_RANKING_COUNT        int
          , QUERY_PLAN                 xml
      )
insert into plan_cache_objects_with_text
            SELECT @data_set, @run_type, @run_nbr
             ,'OBJECT-LEVEL OUTPUT - TEXT QUERY PLAN' AS OUTPUT_TYPE
             , X.DATABASE_NAME 
             , X.DATABASE_ID 
             , X.OBJECT_NAME 
             , X.OBJECT_ID 
             , X.PLAN_INSTANCE_ID
             , X.OBJECT_TYPE 
             , X.CACHED_TIME_MS 
             , X.LAST_EXECUTION_TIME_MS 
             , X.CACHE_TO_LAST_EXEC_MINUTES 
             , EXECUTION_COUNT  
             , AVG_TIME_MS   , TOTAL_TIME_MS   , MIN_TIME_MS   , MAX_TIME_MS   
             , AVG_CPU_MS       , TOTAL_CPU_MS       , MIN_CPU            , MAX_CPU            
             , AVG_LOGICAL_READS  , TOTAL_LOGICAL_READS  , MIN_LOGICAL_READS  , MAX_LOGICAL_READS   
             , AVG_PHYSICAL_READS , TOTAL_PHYSICAL_READS , MIN_PHYSICAL_READS , MAX_PHYSICAL_READS  
             , AVG_LOGICAL_WRITES , TOTAL_LOGICAL_WRITES , MIN_LOGICAL_WRITES , MAX_LOGICAL_WRITES  
             , PLAN_HANDLE
             , OVERALL_SCORE
             , DENSE_RANK() over(ORDER BY OVERALL_SCORE) AS OVERALL_ALL_SCORE_RANKING
             , TOP_N_RANKING_COUNT
             , @TOP_N_OBJECT_RANKINGS_CHECKED AS TOP_N_RANKINGS_CHECKED
             , case when TOP_N_RANKING_COUNT = 0 then 0 else CAST(OVERALL_SCORE * 100.0/TOP_N_RANKING_COUNT AS DECIMAL(12, 2))end AS RANKINGS_MET_PCT
             , EXECUTION_COUNT_RANK    
             --, AVG_TIME_RANK   , TOTAL_TIME_RANK   , MIN_TIME_RANK   , MAX_TIME_RANK   
             --, AVG_CPU_MS_RANK       , TOTAL_CPU_MS_RANK       , MIN_CPU_RANK            , MAX_CPU_RANK            
             --, AVG_LOGICAL_READS_RANK  , TOTAL_LOGICAL_READS_RANK  , MIN_LOGICAL_READS_RANK  , MAX_LOGICAL_READS_RANK   
             --, AVG_PHYSICAL_READS_RANK , TOTAL_PHYSICAL_READS_RANK , MIN_PHYSICAL_READS_RANK , MAX_PHYSICAL_READS_RANK  
             --, AVG_LOGICAL_WRITES_RANK , TOTAL_LOGICAL_WRITES_RANK , MIN_LOGICAL_WRITES_RANK , MAX_LOGICAL_WRITES_RANK
             --, STATEMENT_LEVEL_OVERALL_SCORE  
             , Y.QUERY_PLAN
            FROM #OBJECT_STATS2 X
            outer APPLY SYS.DM_EXEC_TEXT_QUERY_PLAN(PLAN_HANDLE, 0, -1) Y
            WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
            ORDER BY DATABASE_NAME, OBJECT_NAME, PLAN_HANDLE
         END --if @INCLUDE_OBJECT_QUERY_PLANS = 1 AND @RETURN_PLANS_AS_XML_OR_TEXT = N'TEXT'

      if @INCLUDE_OBJECT_QUERY_PLANS = 0
         BEGIN

IF OBJECT_ID(N'TEMPDB..plan_cache_objects') IS null
      CREATE TABLE plan_cache_objects
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , OUTPUT_TYPE                varchar(35)
          , DATABASE_NAME              nvarchar(128)
          , DATABASE_ID                int
          , OBJECT_NAME                nvarchar(128)
          , OBJECT_ID                  int
          , PLAN_INSTANCE_ID           int
          , OBJECT_TYPE                nvarchar(60)
          , CACHED_TIME_MS             datetime
          , LAST_EXECUTION_TIME_MS     datetime
          , CACHE_TO_LAST_EXEC_MINUTES int
          , EXECUTION_COUNT            bigint
          , AVG_TIME_MS                decimal(38,2)
          , TOTAL_TIME_MS              decimal(38,2)
          , MIN_TIME_MS                decimal(38,2)
          , MAX_TIME_MS                decimal(38,2)
          , AVG_CPU_MS                 decimal(38,2)
          , TOTAL_CPU_MS               decimal(38,2)
          , MIN_CPU                    decimal(38,2)
          , MAX_CPU                    decimal(38,2)
          , AVG_LOGICAL_READS          decimal(38,2)
          , TOTAL_LOGICAL_READS        bigint
          , MIN_LOGICAL_READS          bigint
          , MAX_LOGICAL_READS          bigint
          , AVG_PHYSICAL_READS         decimal(38,2)
          , TOTAL_PHYSICAL_READS       bigint
          , MIN_PHYSICAL_READS         bigint
          , MAX_PHYSICAL_READS         bigint
          , AVG_LOGICAL_WRITES         decimal(38,2)
          , TOTAL_LOGICAL_WRITES       bigint
          , MIN_LOGICAL_WRITES         bigint
          , MAX_LOGICAL_WRITES         bigint
          , PLAN_HANDLE                varbinary(64)
          , OVERALL_SCORE              int
          , OVERALL_ALL_SCORE_RANKING  bigint
          , TOP_N_RANKING_COUNT        int
          , TOP_N_RANKINGS_CHECKED     int
          , RANKINGS_MET_PCT           decimal(12,2)
          , EXECUTION_COUNT_RANK       int
      )
  insert into plan_cache_objects
            SELECT @data_set, @run_type, @run_nbr
             ,'OBJECT-LEVEL OUTPUT - NO QUERY PLAN' AS OUTPUT_TYPE
             , X.DATABASE_NAME 
             , X.DATABASE_ID 
             , X.OBJECT_NAME 
             , X.OBJECT_ID 
             , X.PLAN_INSTANCE_ID
             , X.OBJECT_TYPE 
             , X.CACHED_TIME_MS 
             , X.LAST_EXECUTION_TIME_MS 
             , X.CACHE_TO_LAST_EXEC_MINUTES 
             , EXECUTION_COUNT  
             , AVG_TIME_MS   , TOTAL_TIME_MS   , MIN_TIME_MS   , MAX_TIME_MS   
             , AVG_CPU_MS       , TOTAL_CPU_MS       , MIN_CPU            , MAX_CPU            
             , AVG_LOGICAL_READS  , TOTAL_LOGICAL_READS  , MIN_LOGICAL_READS  , MAX_LOGICAL_READS   
             , AVG_PHYSICAL_READS , TOTAL_PHYSICAL_READS , MIN_PHYSICAL_READS , MAX_PHYSICAL_READS  
             , AVG_LOGICAL_WRITES , TOTAL_LOGICAL_WRITES , MIN_LOGICAL_WRITES , MAX_LOGICAL_WRITES  
             , PLAN_HANDLE
             , OVERALL_SCORE
             , DENSE_RANK() OVER(ORDER BY OVERALL_SCORE) AS OVERALL_ALL_SCORE_RANKING
             --, STATEMENT_LEVEL_OVERALL_SCORE  
             , TOP_N_RANKING_COUNT
             , @TOP_N_OBJECT_RANKINGS_CHECKED AS TOP_N_RANKINGS_CHECKED
             , case when TOP_N_RANKING_COUNT = 0 then 0 else CAST(OVERALL_SCORE * 100.0/TOP_N_RANKING_COUNT AS DECIMAL(12, 2))end AS RANKINGS_MET_PCT
             , EXECUTION_COUNT_RANK    
             --, AVG_TIME_RANK   , TOTAL_TIME_RANK   , MIN_TIME_RANK   , MAX_TIME_RANK   
             --, AVG_CPU_MS_RANK       , TOTAL_CPU_MS_RANK       , MIN_CPU_RANK            , MAX_CPU_RANK            
             --, AVG_LOGICAL_READS_RANK  , TOTAL_LOGICAL_READS_RANK  , MIN_LOGICAL_READS_RANK  , MAX_LOGICAL_READS_RANK   
             --, AVG_PHYSICAL_READS_RANK , TOTAL_PHYSICAL_READS_RANK , MIN_PHYSICAL_READS_RANK , MAX_PHYSICAL_READS_RANK  
             --, AVG_LOGICAL_WRITES_RANK , TOTAL_LOGICAL_WRITES_RANK , MIN_LOGICAL_WRITES_RANK , MAX_LOGICAL_WRITES_RANK  

            FROM #OBJECT_STATS2 X
            WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
            ORDER BY DATABASE_NAME, OBJECT_NAME, PLAN_HANDLE
         END -- if @INCLUDE_OBJECT_QUERY_PLANS = 0

      IF @SHOW_OBJECT_STATEMENT_DETAILS = 1
         BEGIN

            SET @SQL_TEXT =  ''
            DECLARE @sqlmajorver int, @sqlminorver int, @sqlbuild int
            SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);
            SELECT @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff);
            SELECT @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

if not exists (select * from sys.tables where name = 'plan_cache_stmt_details')
CREATE TABLE [dbo].plan_cache_stmt_details
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int,

	[OUTPUT_TYPE] [varchar](29) NOT NULL,
	[DATABASE_NAME] [sysname] NOT NULL,
	[OBJECT_NAME] [sysname] NOT NULL,
	[OBJECT_TYPE] [nvarchar](60) NULL,
	[PLAN_INSTANCE_ID] [int] NULL,
	[LAST_EXECUTION_TIME] [datetime] NULL,
	[ROW_NUM] [bigint] NULL,
	[EXECUTION_COUNT] [bigint] NULL,
	[AVG_TIME_MS] [numeric](38, 18) NULL,
	[TOTAL_TIME_MS] [numeric](26, 6) NULL,
	[MIN_TIME_MS] [numeric](26, 6) NULL,
	[MAX_TIME_MS] [numeric](26, 6) NULL,
	[AVG_CPU_MS] [numeric](38, 18) NULL,
	[TOTAL_CPU_MS] [numeric](26, 6) NULL,
	[MIN_CPU_MS] [numeric](26, 6) NULL,
	[MAX_CPU_MS] [numeric](26, 6) NULL,
	[AVG_LOGICAL_READS] [numeric](22, 1) NULL,
	[TOTAL_LOGICAL_READS] [bigint] NULL,
	[MIN_LOGICAL_READS] [bigint] NULL,
	[MAX_LOGICAL_READS] [bigint] NULL,
	[AVG_PHYSICAL_READS] [numeric](22, 1) NULL,
	[TOTAL_PHYSICAL_READS] [bigint] NULL,
	[MIN_PHYSICAL_READS] [bigint] NULL,
	[MAX_PHYSICAL_READS] [bigint] NULL,
	[AVG_LOGICAL_WRITES] [numeric](22, 1) NULL,
	[TOTAL_LOGICAL_WRITES] [bigint] NULL,
	[MIN_LOGICAL_WRITES] [bigint] NULL,
	[MAX_LOGICAL_WRITES] [bigint] NULL,
	[TOTAL_ROWS] [bigint] NULL,
	[LAST_ROWS] [bigint] NULL,
	[MIN_ROWS] [bigint] NULL,
	[MAX_ROWS] [bigint] NULL,
	[total_dop] [int] NULL,
	[last_dop] [int] NULL,
	[min_dop] [int] NULL,
	[max_dop] [int] NULL,
	[total_grant_kb] [int] NULL,
	[last_grant_kb] [int] NULL,
	[min_grant_kb] [int] NULL,
	[max_grant_kb] [int] NULL,
	[total_used_grant_kb] [int] NULL,
	[last_used_grant_kb] [int] NULL,
	[min_used_grant_kb] [int] NULL,
	[max_used_grant_kb] [int] NULL,
	[total_ideal_grant_kb] [int] NULL,
	[last_ideal_grant_kb] [int] NULL,
	[min_ideal_grant_kb] [int] NULL,
	[max_ideal_grant_kb] [int] NULL,
	[total_reserved_threads] [int] NULL,
	[last_reserved_threads] [int] NULL,
	[min_reserved_threads] [int] NULL,
	[max_reserved_threads] [int] NULL,
	[total_used_threads] [int] NULL,
	[last_used_threads] [int] NULL,
	[min_used_threads] [int] NULL,
	[max_used_threads] [int] NULL,
	[PLAN_HANDLE] [varbinary](64) NULL,
	[QUERY_TEXT] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]



  insert into plan_cache_stmt_details
 SELECT @data_set, @run_type, @run_nbr,
 'OBJECT STATEMENT-LEVEL OUTPUT' AS OUTPUT_TYPE
    , X.DATABASE_NAME
    , X.OBJECT_NAME
    , X.OBJECT_TYPE
    , X.PLAN_INSTANCE_ID
    , QS.LAST_EXECUTION_TIME
    , ROW_NUMBER() OVER(PARTITION BY QS.PLAN_HANDLE ORDER BY QS.STATEMENT_START_OFFSET) AS ROW_NUM
    , QS.EXECUTION_COUNT
    , QS.TOTAL_ELAPSED_TIME/1000.0/QS.EXECUTION_COUNT  AS AVG_TIME_MS
    , QS.TOTAL_ELAPSED_TIME/1000.0                     AS TOTAL_TIME_MS
    , QS.MIN_ELAPSED_TIME/1000.0                       AS MIN_TIME_MS
    , QS.MAX_ELAPSED_TIME/1000.0                       AS MAX_TIME_MS
    , QS.TOTAL_WORKER_TIME/1000.0/QS.EXECUTION_COUNT   AS AVG_CPU_MS
    , QS.TOTAL_WORKER_TIME/1000.0                      AS TOTAL_CPU_MS
    , QS.MIN_WORKER_TIME/1000.0                        AS MIN_CPU_MS
    , QS.MAX_WORKER_TIME/1000.0                        AS MAX_CPU_MS
    , QS.TOTAL_LOGICAL_READS/QS.EXECUTION_COUNT  *1.0  AS AVG_LOGICAL_READS
    , QS.TOTAL_LOGICAL_READS
    , QS.MIN_LOGICAL_READS
    , QS.MAX_LOGICAL_READS
    , QS.TOTAL_PHYSICAL_READS/QS.EXECUTION_COUNT  *1.0 AS AVG_PHYSICAL_READS
    , QS.TOTAL_PHYSICAL_READS
    , QS.MIN_PHYSICAL_READS
    , QS.MAX_PHYSICAL_READS
    , QS.TOTAL_LOGICAL_WRITES/QS.EXECUTION_COUNT *1.0  AS AVG_LOGICAL_WRITES
    , QS.TOTAL_LOGICAL_WRITES
    , QS.MIN_LOGICAL_WRITES
    , QS.MAX_LOGICAL_WRITES
    , QS.TOTAL_ROWS
    , QS.LAST_ROWS
    , QS.MIN_ROWS
    , QS.MAX_ROWS

    , NULL as total_dop              
    , NULL as last_dop               
    , NULL as min_dop                
    , NULL as max_dop                
    , NULL as total_grant_kb         
    , NULL as last_grant_kb          
    , NULL as min_grant_kb           
    , NULL as max_grant_kb           
    , NULL as total_used_grant_kb    
    , NULL as last_used_grant_kb     
    , NULL as min_used_grant_kb      
    , NULL as max_used_grant_kb      
    , NULL as total_ideal_grant_kb   
    , NULL as last_ideal_grant_kb    
    , NULL as min_ideal_grant_kb     
    , NULL as max_ideal_grant_kb     
    , NULL as total_reserved_threads 
    , NULL as last_reserved_threads  
    , NULL as min_reserved_threads   
    , NULL as max_reserved_threads   
    , NULL as total_used_threads     
    , NULL as last_used_threads      
    , NULL as min_used_threads       
    , NULL as max_used_threads  

    , QS.PLAN_HANDLE
    , REPLACE(REPLACE(REPLACE(     SUBSTRING(QT.TEXT,QS.STATEMENT_START_OFFSET/2 +1, 
               (CASE WHEN QS.STATEMENT_END_OFFSET = -1 
                     THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2 
                     ELSE QS.STATEMENT_END_OFFSET END -
                          QS.STATEMENT_START_OFFSET
               )/2
           ), CHAR(10), ' '), CHAR(13), ' '), CHAR(9) , ' ') AS QUERY_TEXT
 FROM #OBJECT_STATS2 X 
 --JOIN #STATEMENT_LEVEL SL ON SL.PLAN_HANDLE = X.PLAN_HANDLE
 left JOIN SYS.DM_EXEC_QUERY_STATS QS ON QS.PLAN_HANDLE = X.PLAN_HANDLE-- AND SL.sql_handle = QS.sql_handle AND SL.statement_start_offset = QS.statement_start_offset AND QS.statement_end_offset = SL.statement_end_offset
 outer APPLY SYS.DM_EXEC_SQL_TEXT(QS.SQL_HANDLE) AS QT 
 WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
 ORDER BY object_NAME, PLAN_HANDLE, ROW_NUM

                  --end

            --set @SQL_TEXT = REPLACE(@sql_text, '@APPLY_NO_TOP_FILTERS', @APPLY_NO_TOP_FILTERS)
            --print @sql_text
            --exec (@sql_text)
                     END --IF @SHOW_OBJECT_STATEMENT_DETAILS = 1'

      IF @SHOW_AD_HOC = 1
         BEGIN
            IF OBJECT_ID(N'TEMPDB..#ALL_QUERY_STATS_ENTRIES') is not null DROP TABLE #ALL_QUERY_STATS_ENTRIES

                  SELECT  QUERY_HASH
                  , QUERY_PLAN_HASH
                  , COUNT(*) AS CACHE_ENTIES
                  , SUM(QS.EXECUTION_COUNT) AS EXECUTION_COUNT
                  , MIN(QS.CREATION_TIME) AS MIN_CACHE_DATETIME
                  , MAX(QS.CREATION_TIME)  AS MAX_CACHE_DATETIME
                  , MIN(QS.LAST_EXECUTION_TIME) AS MIN_LAST_EXECUTION_TIME
                  , MAX(QS.LAST_EXECUTION_TIME) AS MAX_LAST_EXECUTION_TIME
                  , SUM(QS.TOTAL_ELAPSED_TIME/1000.0)/SUM(QS.EXECUTION_COUNT)  AS AVG_TIME_MS
                  , SUM(QS.TOTAL_ELAPSED_TIME/1000.0 ) AS  TOTAL_TIME_MS
                  , MIN(QS.MIN_ELAPSED_TIME/1000.0 ) AS MIN_TIME_MS
                  , MAX(QS.MAX_ELAPSED_TIME/1000.0 ) AS MAX_TIME_MS
                  , SUM(QS.TOTAL_WORKER_TIME/1000.0)/SUM(QS.EXECUTION_COUNT)   AS AVG_CPU_MS
                  , SUM(QS.TOTAL_WORKER_TIME/1000.0) AS TOTAL_CPU_MS
                  , MIN(QS.MIN_WORKER_TIME/1000.0) AS MIN_CPU_MS
                  , MAX(QS.MAX_WORKER_TIME/1000.0) AS MAX_CPU_MS
                  , SUM(QS.TOTAL_LOGICAL_READS) *1.0/SUM(QS.EXECUTION_COUNT)  AS AVG_LOGICAL_READS
                  , SUM(QS.TOTAL_LOGICAL_READS ) AS TOTAL_LOGICAL_READS
                  , MIN(QS.MIN_LOGICAL_READS ) AS MIN_LOGICAL_READS
                  , MAX(QS.MAX_LOGICAL_READS ) AS MAX_LOGICAL_READS
                  , SUM(QS.TOTAL_PHYSICAL_READS *1.0)/SUM(QS.EXECUTION_COUNT)  AS AVG_PHYSICAL_READS
                  , SUM(QS.TOTAL_PHYSICAL_READS ) AS TOTAL_PHYSICAL_READS
                  , MIN(QS.MIN_PHYSICAL_READS ) AS MIN_PHYSICAL_READS
                  , MAX(QS.MAX_PHYSICAL_READS ) AS MAX_PHYSICAL_READS
                  , SUM(QS.TOTAL_LOGICAL_WRITES*1.0)/SUM(QS.EXECUTION_COUNT)   AS AVG_LOGICAL_WRITES
                  , SUM(QS.TOTAL_LOGICAL_WRITES ) AS TOTAL_LOGICAL_WRITES
                  , MIN(QS.MIN_LOGICAL_WRITES ) AS MIN_LOGICAL_WRITES
                  , MAX(QS.MAX_LOGICAL_WRITES ) AS MAX_LOGICAL_WRITES
                  , SUM(QS.TOTAL_ROWS ) AS TOTAL_ROWS
                  , SUM(QS.TOTAL_ROWS*1.0)/SUM(QS.EXECUTION_COUNT) AS AVG_ROWS
                  , MIN(QS.MIN_ROWS ) AS MIN_ROWS
                  , MAX(QS.MAX_ROWS ) AS MAX_ROWS

                  , MAX(QS.sql_handle) AS SAMPLE_SQL_HANDLE
                  , MAX(QS.PLAN_HANDLE) as sample_plan_handle

                  , RANK() OVER(ORDER BY COUNT(*)  DESC)                                                   AS CACHE_ENTIES_RANK
                  , RANK() OVER(ORDER BY SUM(QS.EXECUTION_COUNT)  DESC)                                    AS EXECUTION_COUNT_RANK
                  , RANK() over(order by sum(qs.total_ELAPSED_time)*1.0 / sum(qs.execution_count) DESC)        as AVG_TIME_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_ELAPSED_TIME )  DESC)                                AS TOTAL_TIME_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_ELAPSED_TIME )  DESC)                                  AS MIN_TIME_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_ELAPSED_TIME )  DESC)                                  AS MAX_TIME_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_WORKER_TIME*1.0)/SUM(QS.EXECUTION_COUNT)    DESC)    AS AVG_CPU_MS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_WORKER_TIME)   DESC)                                 AS TOTAL_CPU_MS_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_WORKER_TIME)  DESC)                                    AS MIN_CPU_MS_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_WORKER_TIME)  DESC)                                    AS MAX_CPU_MS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_LOGICAL_READS) *1.0/SUM(QS.EXECUTION_COUNT)  DESC)   AS AVG_LOGICAL_READS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_LOGICAL_READS )  DESC)                               AS TOTAL_LOGICAL_READS_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_LOGICAL_READS )   DESC)                                AS MIN_LOGICAL_READS_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_LOGICAL_READS )   DESC)                                AS MAX_LOGICAL_READS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_PHYSICAL_READS *1.0)/SUM(QS.EXECUTION_COUNT)   DESC) AS AVG_PHYSICAL_READS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_PHYSICAL_READS )  DESC)                              AS TOTAL_PHYSICAL_READS_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_PHYSICAL_READS )  DESC)                                AS MIN_PHYSICAL_READS_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_PHYSICAL_READS ) DESC)                                 AS MAX_PHYSICAL_READS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_LOGICAL_WRITES*1.0)/SUM(QS.EXECUTION_COUNT)  DESC)   AS AVG_LOGICAL_WRITES_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_LOGICAL_WRITES ) DESC)                               AS TOTAL_LOGICAL_WRITES_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_LOGICAL_WRITES )  DESC)                                AS MIN_LOGICAL_WRITES_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_LOGICAL_WRITES ) DESC)                                 AS MAX_LOGICAL_WRITES_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_ROWS )  DESC)                                        AS TOTAL_ROWS_RANK
                  , RANK() OVER(ORDER BY SUM(QS.TOTAL_ROWS*1.0)/SUM(QS.EXECUTION_COUNT)  DESC)             AS AVG_ROWS_RANK
                  , RANK() OVER(ORDER BY MIN(QS.MIN_ROWS ) DESC)                                           AS MIN_ROWS_RANK
                  , RANK() OVER(ORDER BY MAX(QS.MAX_ROWS ) DESC)                                           AS MAX_ROWS_RANK


            INTO #ALL_QUERY_STATS_ENTRIES       
            FROM SYS.DM_EXEC_QUERY_STATS QS 
            LEFT JOIN #ALL_OBJECT_STATS O ON QS.PLAN_HANDLE = O.PLAN_HANDLE 
            WHERE O.PLAN_HANDLE IS NULL
            GROUP BY QUERY_HASH
                  , QUERY_PLAN_HASH




         IF OBJECT_ID(N'TEMPDB..#AD_HOC_RANK_FILTERED') IS NOT NULL DROP TABLE #AD_HOC_RANK_FILTERED

         SELECT *
            ,  CASE WHEN @RETURN_TOP_CACHED_ENTRIES         = 1 THEN CACHE_ENTIES_RANK         ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_COUNT  = 1 THEN EXECUTION_COUNT_RANK      ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_EXECUTION_TIME_MS     = 1 THEN AVG_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_TIME_MS   = 1 THEN TOTAL_TIME_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_EXECUTION_TIME_MS         = 1 THEN MAX_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_MAX_EXECUTION_TIME_MS         = 1 THEN MIN_TIME_RANK     ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_CPU                = 1 THEN AVG_CPU_MS_RANK         ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_CPU              = 1 THEN TOTAL_CPU_MS_RANK       ELSE 0 END
             + CASE WHEN @RETURN_MAX_CPU                    = 1 THEN MAX_CPU_MS_RANK         ELSE 0 END
             + CASE WHEN @RETURN_MIN_CPU                    = 1 THEN MIN_CPU_MS_RANK         ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_PHYSICAL_READS     = 1 THEN AVG_LOGICAL_READS_RANK    ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_PHYSICAL_READS   = 1 THEN TOTAL_LOGICAL_READS_RANK  ELSE 0 END
             + CASE WHEN @RETURN_MAX_PHYSICAL_READS         = 1 THEN MAX_LOGICAL_READS_RANK    ELSE 0 END
             + CASE WHEN @RETURN_MIN_PHYSICAL_READS         = 1 THEN MIN_LOGICAL_READS_RANK    ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_READS      = 1 THEN AVG_PHYSICAL_READS_RANK   ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_READS    = 1 THEN TOTAL_PHYSICAL_READS_RANK ELSE 0 END
             + CASE WHEN @RETURN_MAX_LOGICAL_READS          = 1 THEN MAX_PHYSICAL_READS_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_LOGICAL_READS          = 1 THEN MIN_PHYSICAL_READS_RANK   ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_WRITES     = 1 THEN AVG_LOGICAL_WRITES_RANK   ELSE 0 END            
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_WRITES   = 1 THEN TOTAL_LOGICAL_WRITES_RANK ELSE 0 END
             + CASE WHEN @RETURN_MAX_LOGICAL_WRITES         = 1 THEN MAX_LOGICAL_WRITES_RANK   ELSE 0 END
             + CASE WHEN @RETURN_MIN_LOGICAL_WRITES         = 1 THEN MIN_LOGICAL_WRITES_RANK   ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_ROWS               = 1 THEN TOTAL_ROWS_RANK           ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_ROWS             = 1 THEN AVG_ROWS_RANK             ELSE 0 END 
             + CASE WHEN @RETURN_MAX_ROWS                   = 1 THEN MIN_ROWS_RANK             ELSE 0 END 
             + CASE WHEN @RETURN_MIN_ROWS                   = 1 THEN MAX_ROWS_RANK             ELSE 0 END AS OVERALL_SCORE 

            ,  CASE WHEN @RETURN_TOP_CACHED_ENTRIES         = 1 AND CACHE_ENTIES_RANK         <= @TOP_N_VALUE THEN 1 ELSE 0 END
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_COUNT  = 1 AND EXECUTION_COUNT_RANK      <= @TOP_N_VALUE THEN 1 ELSE 0 END
             + CASE WHEN @RETURN_TOP_AVG_EXECUTION_TIME_MS     = 1 AND AVG_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_EXECUTION_TIME_MS   = 1 AND TOTAL_TIME_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_EXECUTION_TIME_MS         = 1 AND MAX_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_EXECUTION_TIME_MS         = 1 AND MIN_TIME_RANK     <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_CPU                = 1 AND AVG_CPU_MS_RANK         <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_CPU              = 1 AND TOTAL_CPU_MS_RANK       <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_CPU                    = 1 AND MAX_CPU_MS_RANK         <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_CPU                    = 1 AND MIN_CPU_MS_RANK         <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_PHYSICAL_READS     = 1 AND AVG_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_PHYSICAL_READS   = 1 AND TOTAL_LOGICAL_READS_RANK  <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_PHYSICAL_READS         = 1 AND MAX_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_PHYSICAL_READS         = 1 AND MIN_LOGICAL_READS_RANK    <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_READS      = 1 AND AVG_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_READS    = 1 AND TOTAL_PHYSICAL_READS_RANK <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_LOGICAL_READS          = 1 AND MAX_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_LOGICAL_READS          = 1 AND MIN_PHYSICAL_READS_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_LOGICAL_WRITES     = 1 AND AVG_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_LOGICAL_WRITES   = 1 AND TOTAL_LOGICAL_WRITES_RANK <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_LOGICAL_WRITES         = 1 AND MAX_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_LOGICAL_WRITES         = 1 AND MIN_LOGICAL_WRITES_RANK   <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_AVG_ROWS               = 1 AND TOTAL_ROWS_RANK           <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_TOP_TOTAL_ROWS             = 1 AND AVG_ROWS_RANK             <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MAX_ROWS                   = 1 AND MIN_ROWS_RANK             <= @TOP_N_VALUE THEN 1 ELSE 0 END 
             + CASE WHEN @RETURN_MIN_ROWS                   = 1 AND MAX_ROWS_RANK             <= @TOP_N_VALUE THEN 1 ELSE 0 END AS TOP_N_RANKING_COUNT 
         INTO #AD_HOC_RANK_FILTERED
         FROM #ALL_QUERY_STATS_ENTRIES
         WHERE 
         @APPLY_NO_TOP_FILTERS = 1 OR
         (
            (@RETURN_TOP_CACHED_ENTRIES           = 1 AND CACHE_ENTIES_RANK              <= @TOP_N_VALUE)
         OR (@RETURN_TOP_TOTAL_EXECUTION_COUNT    = 1 and EXECUTION_COUNT_RANK           <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_EXECUTION_TIME_MS       = 1 AND AVG_TIME_RANK          <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_EXECUTION_TIME_MS     = 1 AND TOTAL_TIME_RANK        <= @TOP_N_VALUE)
         or (@RETURN_MAX_EXECUTION_TIME_MS           = 1 AND MIN_TIME_RANK          <= @TOP_N_VALUE)
         or (@RETURN_MIN_EXECUTION_TIME_MS           = 1 AND MAX_TIME_RANK          <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_CPU                  = 1 AND AVG_CPU_MS_RANK              <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_CPU                = 1 AND TOTAL_CPU_MS_RANK            <= @TOP_N_VALUE)
         or (@RETURN_MAX_CPU                      = 1 AND MIN_CPU_MS_RANK              <= @TOP_N_VALUE)
         or (@RETURN_MIN_CPU                      = 1 AND MAX_CPU_MS_RANK              <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_PHYSICAL_READS       = 1 AND AVG_LOGICAL_READS_RANK         <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_PHYSICAL_READS     = 1 AND TOTAL_LOGICAL_READS_RANK       <= @TOP_N_VALUE)
         or (@RETURN_MAX_PHYSICAL_READS           = 1 AND MIN_LOGICAL_READS_RANK         <= @TOP_N_VALUE)
         or (@RETURN_MIN_PHYSICAL_READS           = 1 AND MAX_LOGICAL_READS_RANK         <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_LOGICAL_READS        = 1 AND AVG_PHYSICAL_READS_RANK        <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_LOGICAL_READS      = 1 AND TOTAL_PHYSICAL_READS_RANK      <= @TOP_N_VALUE)
         or (@RETURN_MAX_LOGICAL_READS            = 1 AND MIN_PHYSICAL_READS_RANK        <= @TOP_N_VALUE)
         or (@RETURN_MIN_LOGICAL_READS            = 1 AND MAX_PHYSICAL_READS_RANK        <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_LOGICAL_WRITES       = 1 AND AVG_LOGICAL_WRITES_RANK        <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_LOGICAL_WRITES     = 1 AND TOTAL_LOGICAL_WRITES_RANK      <= @TOP_N_VALUE)
         or (@RETURN_MAX_LOGICAL_WRITES           = 1 AND MIN_LOGICAL_WRITES_RANK        <= @TOP_N_VALUE)
         or (@RETURN_MIN_LOGICAL_WRITES           = 1 AND MAX_LOGICAL_WRITES_RANK        <= @TOP_N_VALUE)
         or (@RETURN_TOP_AVG_ROWS                 = 1 AND TOTAL_ROWS_RANK                <= @TOP_N_VALUE)
         or (@RETURN_TOP_TOTAL_ROWS               = 1 AND AVG_ROWS_RANK                  <= @TOP_N_VALUE)
         or (@RETURN_MAX_ROWS                     = 1 AND MIN_ROWS_RANK                  <= @TOP_N_VALUE)
         or (@RETURN_MIN_ROWS                     = 1 AND MAX_ROWS_RANK                  <= @TOP_N_VALUE))


/* GET SAMPLE QUERY PLAN AND SQL_TEXT*/
IF @INCLUDE_AD_HOC_QUERY_PLANS = 1
   BEGIN  

IF OBJECT_ID(N'TEMPDB..plan_cache_ad_hoc_with_plans') IS  NULL 
      CREATE TABLE plan_cache_ad_hoc_with_plans
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , QUERY_HASH              binary(8)
          , QUERY_PLAN_HASH         binary(8)
          , QUERY_TEXT              nvarchar(MAX)
          , query_plan              nvarchar(MAX)
          , CACHE_ENTIES            int
          , EXECUTION_COUNT         bigint
          , MIN_CACHE_DATETIME      datetime
          , MAX_CACHE_DATETIME      datetime
          , MIN_LAST_EXECUTION_TIME datetime
          , MAX_LAST_EXECUTION_TIME datetime
          , AVG_TIME_MS             numeric(38,6)
          , TOTAL_TIME_MS           numeric(38,6)
          , MIN_TIME_MS             numeric(26,6)
          , MAX_TIME_MS             numeric(26,6)
          , AVG_CPU_MS              numeric(38,6)
          , TOTAL_CPU_MS            numeric(38,6)
          , MIN_CPU_MS              numeric(26,6)
          , MAX_CPU_MS              numeric(26,6)
          , AVG_LOGICAL_READS       numeric(38,17)
          , TOTAL_LOGICAL_READS     bigint
          , MIN_LOGICAL_READS       bigint
          , MAX_LOGICAL_READS       bigint
          , AVG_PHYSICAL_READS      numeric(38,6)
          , TOTAL_PHYSICAL_READS    bigint
          , MIN_PHYSICAL_READS      bigint
          , MAX_PHYSICAL_READS      bigint
          , AVG_LOGICAL_WRITES      numeric(38,6)
          , TOTAL_LOGICAL_WRITES    bigint
          , MIN_LOGICAL_WRITES      bigint
          , MAX_LOGICAL_WRITES      bigint
          , TOTAL_ROWS              bigint
          , AVG_ROWS                numeric(38,6)
          , MIN_ROWS                bigint
          , MAX_ROWS                bigint
          , SAMPLE_SQL_HANDLE       varbinary(64)
          , sample_plan_handle      varbinary(64)
      )
 insert into plan_cache_ad_hoc_with_plans
            SELECT @data_set, @run_type, @run_nbr
             ,
         Q.QUERY_HASH
       , Q.QUERY_PLAN_HASH
       , REPLACE(REPLACE(REPLACE(     SUBSTRING(QT.TEXT,QS.STATEMENT_START_OFFSET/2 +1, 
                     (CASE WHEN QS.STATEMENT_END_OFFSET = -1 
                           THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2 
                           ELSE QS.STATEMENT_END_OFFSET END -
                                 QS.STATEMENT_START_OFFSET
                     )/2
                  ), CHAR(10), ' '), CHAR(13), ' '), CHAR(9) , ' ') AS QUERY_TEXT
       , Y.query_plan
       , Q.CACHE_ENTIES
       , Q.EXECUTION_COUNT
       , Q.MIN_CACHE_DATETIME
       , Q.MAX_CACHE_DATETIME    
       , Q.MIN_LAST_EXECUTION_TIME  
       , Q.MAX_LAST_EXECUTION_TIME  
       , Q.AVG_TIME_MS
       , Q.TOTAL_TIME_MS
       , Q.MIN_TIME_MS
       , Q.MAX_TIME_MS
       , Q.AVG_CPU_MS
       , Q.TOTAL_CPU_MS
       , Q.MIN_CPU_MS
       , Q.MAX_CPU_MS
       , Q.AVG_LOGICAL_READS
       , Q.TOTAL_LOGICAL_READS
       , Q.MIN_LOGICAL_READS
       , Q.MAX_LOGICAL_READS
       , Q.AVG_PHYSICAL_READS
       , Q.TOTAL_PHYSICAL_READS
       , Q.MIN_PHYSICAL_READS
       , Q.MAX_PHYSICAL_READS
       , Q.AVG_LOGICAL_WRITES
       , Q.TOTAL_LOGICAL_WRITES
       , Q.MIN_LOGICAL_WRITES
       , Q.MAX_LOGICAL_WRITES
       , Q.TOTAL_ROWS
       , Q.AVG_ROWS
       , Q.MIN_ROWS
       , Q.MAX_ROWS
       , Q.SAMPLE_SQL_HANDLE
       , sample_plan_handle
         from #AD_HOC_RANK_FILTERED q
         left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handlE
         and qs.query_hash = q.query_hash
         and qs.query_plan_hash = q.query_plan_hash
          outer APPLY SYS.DM_EXEC_SQL_TEXT(Q.SAMPLE_SQL_HANDLE) AS QT 
          outer APPLY SYS.DM_EXEC_TEXT_QUERY_PLAN(SAMPLE_PLAN_HANDLE, 0, -1) Y
         ORDER BY q.query_hash, q.query_plan_hash
   end -- IF @INCLUDE_AD_HOC_QUERY_PLANS = 1


   IF @INCLUDE_AD_HOC_QUERY_PLANS = 0
      BEGIN  

IF OBJECT_ID(N'TEMPDB..plan_cache_ad_hoc') IS NULL 
      CREATE TABLE plan_cache_ad_hoc
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , QUERY_HASH              binary(8)
          , QUERY_PLAN_HASH         binary(8)
          , QUERY_TEXT              nvarchar(MAX)
          , query_plan              varchar(1)
          , CACHE_ENTIES            int
          , EXECUTION_COUNT         bigint
          , MIN_CACHE_DATETIME      datetime
          , MAX_CACHE_DATETIME      datetime
          , MIN_LAST_EXECUTION_TIME datetime
          , MAX_LAST_EXECUTION_TIME datetime
          , AVG_TIME_MS             numeric(38,6)
          , TOTAL_TIME_MS           numeric(38,6)
          , MIN_TIME_MS             numeric(26,6)
          , MAX_TIME_MS             numeric(26,6)
          , AVG_CPU_MS              numeric(38,6)
          , TOTAL_CPU_MS            numeric(38,6)
          , MIN_CPU_MS              numeric(26,6)
          , MAX_CPU_MS              numeric(26,6)
          , AVG_LOGICAL_READS       numeric(38,17)
          , TOTAL_LOGICAL_READS     bigint
          , MIN_LOGICAL_READS       bigint
          , MAX_LOGICAL_READS       bigint
          , AVG_PHYSICAL_READS      numeric(38,6)
          , TOTAL_PHYSICAL_READS    bigint
          , MIN_PHYSICAL_READS      bigint
          , MAX_PHYSICAL_READS      bigint
          , AVG_LOGICAL_WRITES      numeric(38,6)
          , TOTAL_LOGICAL_WRITES    bigint
          , MIN_LOGICAL_WRITES      bigint
          , MAX_LOGICAL_WRITES      bigint
          , TOTAL_ROWS              bigint
          , AVG_ROWS                numeric(38,6)
          , MIN_ROWS                bigint
          , MAX_ROWS                bigint
          , SAMPLE_SQL_HANDLE       varbinary(64)
          , sample_plan_handle      varbinary(64)
      )

insert into plan_cache_ad_hoc
            SELECT @data_set, @run_type, @run_nbr
             ,
            Q.QUERY_HASH
          , Q.QUERY_PLAN_HASH
          , REPLACE(REPLACE(REPLACE(     SUBSTRING(QT.TEXT,QS.STATEMENT_START_OFFSET/2 +1, 
                        (CASE WHEN QS.STATEMENT_END_OFFSET = -1 
                              THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2 
                              ELSE QS.STATEMENT_END_OFFSET END -
                                    QS.STATEMENT_START_OFFSET
                        )/2
                     ), CHAR(10), ' '), CHAR(13), ' '), CHAR(9) , ' ') AS QUERY_TEXT
          , '' as query_plan
          , Q.CACHE_ENTIES
          , Q.EXECUTION_COUNT
          , Q.MIN_CACHE_DATETIME
          , Q.MAX_CACHE_DATETIME  
          , Q.MIN_LAST_EXECUTION_TIME  
          , Q.MAX_LAST_EXECUTION_TIME    
          , Q.AVG_TIME_MS
          , Q.TOTAL_TIME_MS
          , Q.MIN_TIME_MS
          , Q.MAX_TIME_MS
          , Q.AVG_CPU_MS
          , Q.TOTAL_CPU_MS
          , Q.MIN_CPU_MS
          , Q.MAX_CPU_MS
          , Q.AVG_LOGICAL_READS
          , Q.TOTAL_LOGICAL_READS
          , Q.MIN_LOGICAL_READS
          , Q.MAX_LOGICAL_READS
          , Q.AVG_PHYSICAL_READS
          , Q.TOTAL_PHYSICAL_READS
          , Q.MIN_PHYSICAL_READS
          , Q.MAX_PHYSICAL_READS
          , Q.AVG_LOGICAL_WRITES
          , Q.TOTAL_LOGICAL_WRITES
          , Q.MIN_LOGICAL_WRITES
          , Q.MAX_LOGICAL_WRITES
          , Q.TOTAL_ROWS
          , Q.AVG_ROWS
          , Q.MIN_ROWS
          , Q.MAX_ROWS
          , Q.SAMPLE_SQL_HANDLE
          , sample_plan_handle
            from #AD_HOC_RANK_FILTERED q
            left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handlE
            and qs.query_hash = q.query_hash
            and qs.query_plan_hash = q.query_plan_hash
             outer APPLY SYS.DM_EXEC_SQL_TEXT(Q.SAMPLE_SQL_HANDLE) AS QT  
      end --IF @INCLUDE_AD_HOC_QUERY_PLANS = 0
   end --IF @SHOW_AD_HOC = 0  
end
--end
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------

set nocount on

/* WAIT STAT INFORMATION

GRAIN: PER wait type, per time interval.

Because sys.dm_os_wait_stats is cumulative since restart, data have to be collected at two points in 
time and differences calculated.

Results are stored in a temp table and returned after the specified number of loops.

Parameters:
   |-- DATA COLLECTION TYPE:
      |--@SHOW_HISTORICAL_SNAPSHOT:  NO LOOPING.  Dump of sys.dm_os_wait_stats
         and/or
      |--@calc_interval_diffs:       LOOPING.  Compares data for current loop against prior loop and stores diffs.
         |--@loop_count
         |--@loop_interval_seconds

   |-- FILTERS
      |-- @TOP_N                       Return the top 20 wait types by wait_time_ms.  for @calc_interval_diffs, based on sum across all loops
                                       Applied AFTER the next two filters.
      |-- @only_show_changes.          Set to 1 to exclude values where min(calc_value) = max(calc_value).
                                       Since historical snapshot is single data set, @only_show_changes does not apply.
      |-- @only_show_nonzero.          Subset of @only_show_changes.  sometimes you want to see non-changing, non-zero values.
      |-- @exclude_useless_wait_types  Some wait types are not useful (e.g., 'NOWAIT' intervals).  Script includes #excluded_wait_types
                                       table that you can add to or remove from, as you learn more about wait types
*/
-------------------------------------------------------------------
/* DATA COLLECTION PARAMETERS*/

DECLARE @SHOW_HISTORICAL_SNAPSHOT   BIT         =  1 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.

DECLARE @CALC_INTERVAL_DIFFS        BIT         =  0 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @LOOP_COUNT                 INT         =  5 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS      INT         =  1
                                    
/* FILTER PARAMETERS*/  
DECLARE @TOP_N                      INT         =  15 -- RETURN TOP N BY WAIT_TIME_MS descending.  applied after next two filters
DECLARE @ONLY_SHOW_CHANGES          BIT         =  0 -- THERE ARE A LOT OF COUNTERS.  SET TO 1 TO EXCLUDE VALUES WHERE MIN(CALC_VALUE) = MAX(CALC_VALUE)
DECLARE @ONLY_SHOW_NONZERO          BIT         =  1 -- SUBSET OF LOGIC ABOVE.  SOMETIMES YOU WANT TO SEE NON-CHANGING, NON-ZERO VALUES.
DECLARE @EXCLUDE_USELESS_WAIT_TYPES BIT         =  1 -- LIST OF WAIT STATS THAT FIRE TO POLL FOR WORK, ETC.

-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
/* clear wait stats

DBCC SQLPERF ('sys.dm_os_wait_stats', CLEAR);
*/
-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

/* set unwanted wait types*/
IF OBJECT_ID(N'TEMPDB..#EXCLUDED_WAIT_TYPES') IS NOT NULL DROP TABLE #EXCLUDED_WAIT_TYPES

CREATE TABLE #EXCLUDED_WAIT_TYPES
(wait_type NVARCHAR(60))

if @EXCLUDE_USELESS_WAIT_TYPES = 1
   begin
      insert into #EXCLUDED_WAIT_TYPES select 'broker_eventhandler'
      insert into #excluded_wait_types select 'BROKER_RECEIVE_WAITFOR'
      insert into #excluded_wait_types select 'BROKER_TASK_STOP'
      insert into #excluded_wait_types select 'BROKER_TO_FLUSH'
      insert into #excluded_wait_types select 'CHECKPOINT_QUEUE'
      insert into #excluded_wait_types select 'CLR_AUTO_EVENT'
      insert into #excluded_wait_types select 'CLR_MANUAL_EVENT'
      insert into #excluded_wait_types select 'DIRTY_PAGE_POLL'
      insert into #excluded_wait_types select 'DISPATCHER_QUEUE_SEMAPHORE'
      insert into #excluded_wait_types select 'FT_IFTS_SCHEDULER_IDLE_WAIT'
      insert into #excluded_wait_types select 'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
      insert into #excluded_wait_types select 'LAZYWRITER_SLEEP'
      insert into #excluded_wait_types select 'LOGMGR_QUEUE'
      insert into #excluded_wait_types select 'ONDEMAND_TASK_QUEUE'
      insert into #excluded_wait_types select 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'
      insert into #excluded_wait_types select 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'
      insert into #excluded_wait_types select 'REQUEST_FOR_DEADLOCK_SEARCH'
      insert into #excluded_wait_types select 'SLEEP_TASK'
      insert into #excluded_wait_types select 'SP_SERVER_DIAGNOSTICS_SLEEP'
      insert into #excluded_wait_types select 'SQLTRACE_BUFFER_FLUSH'
      insert into #excluded_wait_types select 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP'
      insert into #excluded_wait_types select 'tracewrite'
      insert into #excluded_wait_types select 'WAITFOR'
      insert into #excluded_wait_types select 'XE_DISPATCHER_WAIT'
      insert into #excluded_wait_types select 'XE_TIMER_EVENT'
   end --if @EXCLUDE_USELESS_WAIT_TYPES = 1
----------------------------------------------------------------------------
/* PREP WORK*/

declare @total_time int= @loop_count * @LOOP_INTERVAL_SECONDS

--select 'Results in ' + cast(@total_time as varchar) + ' seconds.  Check messages tab for progress.'

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin prep work', 10, 1) with nowait





DECLARE @NOW               DATETIME = GETDATE()   --MAKE SURE ALL RECORDS IN THE BATCH ARE INSERTED WITH SAME VALUE
DECLARE @SERVER_INSTANCE    SYSNAME = CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME)
DECLARE @SERVER_START_TIME DATETIME = (SELECT SQLSERVER_START_TIME FROM SYS.DM_OS_SYS_INFO)
DECLARE @DURATION    decimal(34, 3) = DATEDIFF(second, @SERVER_START_TIME, @NOW)
DECLARE @PRIOR_DATETIME    DATETIME = NULL

DECLARE @BATCH_COUNTER INT = 1
DECLARE @ORIGINAL_LOOP_VALUE INT = @LOOP_COUNT

IF OBJECT_ID(N'TEMPDB..#CURRENT') IS NOT NULL DROP TABLE #CURRENT
CREATE TABLE #CURRENT
(     batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , wait_type                      nvarchar(60)
    , waiting_tasks_count            bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
    , signal_wait_time_ms            bigint
) 

IF OBJECT_ID(N'TEMPDB..#PRIOR') IS NOT NULL DROP TABLE #PRIOR
CREATE TABLE #PRIOR
(     prior_id                       int identity(1, 1)
    , batch_id                       int
    , batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , wait_type                      nvarchar(60)
    , waiting_tasks_count            bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
    , signal_wait_time_ms            bigint
) 
IF OBJECT_ID(N'TEMPDB..#HISTORY') IS NOT NULL DROP TABLE #HISTORY
CREATE TABLE #HISTORY
(     history_id                     int identity (1, 1)
    , batch_id                       int
    , batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , wait_type                      nvarchar(60)
    , waiting_tasks_count            bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
    , signal_wait_time_ms            bigint

) 


IF OBJECT_ID(N'TEMPDB..#aggregate') IS NOT NULL DROP TABLE #aggregate
 
CREATE TABLE #aggregate
(
      output_type                 varchar(50)
    , batches                     int
    , wait_type                   nvarchar(60)
    , filter_status               varchar(19)
    , total_waiting_tasks_count   bigint
    , avg_waiting_tasks_count     decimal(34, 2)
    , min_waiting_tasks_count     bigint
    , max_waiting_tasks_count     bigint
    , wait_time_ms                bigint
    , avg_wait_time_ms            decimal(34, 2)
    , min_wait_time_ms            bigint
    , max_wait_time_ms            bigint
    , avg_wait_ms_per_wait        decimal(38,2)
    , signal_wait_time_ms         bigint
    , avg_signal_wait_ms_per_wait decimal(38,2)
)

IF OBJECT_ID(N'TEMPDB..#aggregate_filtered') IS NOT NULL DROP TABLE #aggregate_filtered
 
CREATE TABLE #aggregate_filtered
(
      output_type                 varchar(50)
    , batches                     int
    , wait_type                   nvarchar(60)
    , filter_status               varchar(19)
    , total_waiting_tasks_count   bigint
    , avg_waiting_tasks_count     decimal(34, 2)
    , min_waiting_tasks_count     bigint
    , max_waiting_tasks_count     bigint
    , wait_time_ms                bigint
    , avg_wait_time_ms            decimal(34, 2)
    , min_wait_time_ms            bigint
    , max_wait_time_ms            bigint
    , avg_wait_ms_per_wait        decimal(38,2)
    , signal_wait_time_ms         bigint
    , avg_signal_wait_ms_per_wait decimal(38,2)
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
              @NOW                      AS batch_datetime
            , @DURATION                 AS duration_sec 
            , @SERVER_INSTANCE          AS server_instance 
            , @SERVER_START_TIME        AS server_start_datetime
            , ls.wait_type           
            , ls.waiting_tasks_count
            , ls.wait_time_ms          
            , ls.max_wait_time_ms 
            , ls.signal_wait_time_ms     
         FROM sys.dm_os_wait_stats  ls 
         left join #EXCLUDED_WAIT_TYPES e on e.wait_type = ls.wait_type
         where e.wait_type is null


      raiserror('   |--- End   Insert into #CURRENT ', 10, 1) with nowait

      IF @SHOW_HISTORICAL_SNAPSHOT = 1 and @BATCH_COUNTER = 1
         BEGIN

IF OBJECT_ID(N'TEMPDB..wait_stats') IS  NULL 
   CREATE TABLE tempdb.dbo.wait_stats
      (     data_set                   varchar(50)
          , run_type                   varchar(50)
          , run_nbr                    int
          , OUTPUT_TYPE                      varchar(23)
       , BATCH_ID                         int
       , wait_type                        nvarchar(60)
       , waiting_tasks_count              bigint
       , wait_time_ms                     bigint
       , signal_wait_time_ms              bigint
       , avg_ms_per_wait                  decimal(38,2)
       , avg_signal_wait_time_ms_per_wait decimal(38,2)
       , max_wait_time_ms                 bigint
   )
insert into tempdb.dbo.wait_stats
            SELECT top (@TOP_N) @data_set, @run_type, @run_nbr, 
               'Wait types - Historical' as OUTPUT_TYPE
               , @BATCH_COUNTER AS BATCH_ID
               , c.wait_type
               , waiting_tasks_count
               , wait_time_ms
               , signal_wait_time_ms
               , CAST( case when waiting_tasks_count = 0 then 0 
                           else wait_time_ms *1.0/waiting_tasks_count end as decimal(38, 2)) as avg_ms_per_wait
               , CAST( case when waiting_tasks_count = 0 then 0 
                           else signal_wait_time_ms *1.0/waiting_tasks_count end as decimal(38, 2)) as avg_signal_wait_time_ms_per_wait
               , max_wait_time_ms
                     FROM #CURRENT c
                     left join #excluded_wait_types e on e.wait_type = c.wait_type
                     where @ONLY_SHOW_NONZERO = 0 or ( @ONLY_SHOW_NONZERO = 1 and waiting_tasks_count <> 0)
                     and e.wait_type is null
                     ORDER BY wait_time_ms desc

        raiserror('   |--------- Begin IF @CALC_INTERVAL_DIFFS = 0  ', 10, 1) with nowait

         IF @CALC_INTERVAL_DIFFS = 0 
            BEGIN
               RETURN
            END --IF @CALC_INTERVAL_DIFFS = 0 

        raiserror('   |--------- End   IF @CALC_INTERVAL_DIFFS = 0   ', 10, 1) with nowait

         END --IF @SHOW_HISTORICAL_SNAPSHOT = 1

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
               , C.batch_datetime
               , @DURATION              
               , C.server_instance
               , C.server_start_datetime
               , c.wait_type           
               , coalesce(c.waiting_tasks_count, 0) - coalesce(P.waiting_tasks_count , 0) AS waiting_tasks_count
               , coalesce(C.wait_time_ms , 0)       - coalesce(P.wait_time_ms , 0)        AS wait_time_ms
               , case when coalesce(C.max_wait_time_ms, 0) > coalesce(p.max_wait_time_ms, 0) 
                      then coalesce(C.max_wait_time_ms, 0) 
                      else coalesce(p.max_wait_time_ms, 0) end                            as max_wait_time_ms
               , coalesce(c.signal_wait_time_ms, 0) - coalesce(P.signal_wait_time_ms , 0) AS signal_wait_time_ms
               FROM #CURRENT    C
               FULL JOIN #PRIOR P ON P.wait_type = C.wait_type

            truncate table #prior

            INSERT INTO #PRIOR
               SELECT @BATCH_COUNTER, * FROM #CURRENT
       



         END

      raiserror('   |--- End   IF @ORIGINAL_LOOP_VALUE <> @LOOP_COUNT ', 10, 1) with nowait

      SET @LOOP_COUNT = @LOOP_COUNT - 1
      SET @BATCH_COUNTER = @BATCH_COUNTER + 1
      SET  @PRIOR_DATETIME = @NOW 

      waitfor delay @LOOP_INTERVAL_SECONDS

   END --WHILE @COUNTER <= @LOOP_COUNT
Raiserror('|- End   loop', 10, 1) with nowait
Raiserror('|----------------------------------------------------------------', 10, 1) with nowait

----------------------------------------------------------------------------------------------------

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin output from loop', 10, 1) with nowait

/* get aggregates - used for reporting and for filtering details*/
insert into #aggregate
   select 'Wait Stats - Loops Aggregated' as output_type
   , count(*)                 as batches
   , wait_type
   , case when sum(waiting_tasks_count) = 0 then 'No Waits'
          when min(waiting_tasks_count) = max(waiting_tasks_count) then 'No Changes in Waits' else '' end as filter_status
   , sum(waiting_tasks_count) as total_waiting_tasks_count
   , avg(cast(waiting_tasks_count as decimal(34, 2))) as avg_waiting_tasks_count
   , min(waiting_tasks_count) as min_waiting_tasks_count
   , max(waiting_tasks_count) as max_waiting_tasks_count

   , sum(wait_time_ms       ) as wait_time_ms
   , avg(cast(wait_time_ms  as decimal(34, 2)))   as avg_wait_time_ms
   , min(wait_time_ms       ) as min_wait_time_ms
   , max(wait_time_ms       ) as max_wait_time_ms

   , cast(case when sum(waiting_tasks_count) = 0 then 0
          else sum(wait_time_ms       ) * 1.0 / sum(waiting_tasks_count) end as decimal(38, 2)) as avg_wait_ms_per_wait
   , sum(signal_wait_time_ms) as signal_wait_time_ms
   , cast(case when sum(waiting_tasks_count) = 0 then 0
          else sum(signal_wait_time_ms) * 1.0 / sum(waiting_tasks_count)end as decimal(38, 2)) as avg_signal_wait_ms_per_wait
   from #HISTORY
   where batch_id <> 1
   group by wait_type

/* only show wait stats with any values in time monitored.*/


insert into #aggregate_filtered
   select top (@top_n) * 
   from #aggregate  a
   where (((@ONLY_SHOW_CHANGES = 1 and a.filter_status = '')
           or 
          (@only_show_changes = 0))
         and
          ((@ONLY_SHOW_NONZERO = 1 and a.filter_status in ('', 'No Changes in Waits'))
          or 
         (@ONLY_SHOW_NONZERO = 0)))
   order by wait_time_ms desc

select *
from #aggregate_filtered
order by wait_time_ms desc

SELECT 'Wait types - Loop Details' as output_type
    , h.batch_id
    , h.batch_datetime
    , h.duration_sec
    , h.server_instance
    , h.server_start_datetime
    , h.wait_type
    , h.waiting_tasks_count
    , h.wait_time_ms
    , CAST( case when h.waiting_tasks_count = 0 then 0 
                 else h.wait_time_ms *1.0/h.waiting_tasks_count end as decimal(38, 4)) as avg_ms_per_wait
    , h.signal_wait_time_ms
    , CAST( case when h.waiting_tasks_count = 0 then 0 
                 else h.signal_wait_time_ms *1.0/h.waiting_tasks_count end as decimal(38, 4)) as avg_signal_wait_time_ms_per_wait
    , h.max_wait_time_ms
FROM #HISTORY h
join #aggregate_filtered a on a.wait_type = h.wait_type
where batch_id <> 1
order by wait_type, BATCH_DATETIME

/* select distinct wait_type from #history 
where wait_type in (select wait_type from #HISTORY where BATCH_ID <> 1 group by wait_type having SUM(waiting_tasks_count) > 0)

*/

if @@ROWCOUNT = 0
   begin
      select 'No waits logged during the collection period met the filter criteria.'
   end


Raiserror('|- End   output from loop', 10, 1) with nowait
Raiserror('|----------------------------------------------------------------', 10, 1) with nowait




select * from tempdb.[dbo].[plan_cache_objects]
select * from tempdb.[dbo].[plan_cache_stmt_details]
select * from tempdb.[dbo].[plan_cache_ad_hoc]
select * from tempdb.[dbo].[wait_stats]
--select @data_set, @run_type, @run_nbr, * from 
--select @data_set, @run_type, @run_nbr, * from 

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

skipToEnd:
