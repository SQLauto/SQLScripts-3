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

SET DEADLOCK_PRIORITY LOW
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 

--SET STATISTICS IO ON
--SET STATISTICS TIME_MS ON
--SET STATISTICS IO OFF
--SET STATISTICS TIME_MS OFF
---------------------------------------------------------------------------------------------------------------
BEGIN /*VARIABLE DECLARATION*/
---------------------------------------------------------------------------------------------------------------
/* SELECT OUTPUT TYPES.  CAN MULTI-SELECT*/
DECLARE @RETURN_PLAN_CACHE_SUMMARY       TINYINT = 1
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
         DECLARE @DB_EXCLUDE_LIST NVARCHAR(1000) = 'TEMPDB'

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
            SELECT 'OBJECT-LEVEL OUTPUT - XML QUERY PLAN' AS OUTPUT_TYPE
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
            SELECT 'OBJECT-LEVEL OUTPUT - TEXT QUERY PLAN' AS OUTPUT_TYPE
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
            SELECT 'OBJECT-LEVEL OUTPUT - NO QUERY PLAN' AS OUTPUT_TYPE
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

            if (@sqlmajorver = 11 and ((@sqlminorver = 0 and @sqlbuild >= 6020) or @sqlminorver >= 1))
               or 
               (@sqlmajorver = 12 and (@sqlminorver >= 1))
               or 
               (@sqlmajorver = 13)
                  begin
                     set @SQL_TEXT = '

 SELECT ''OBJECT STATEMENT-LEVEL OUTPUT'' AS OUTPUT_TYPE
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

    , qs.total_dop              
    , qs.last_dop               
    , qs.min_dop                
    , qs.max_dop                
    , qs.total_grant_kb         
    , qs.last_grant_kb          
    , qs.min_grant_kb           
    , qs.max_grant_kb           
    , qs.total_used_grant_kb    
    , qs.last_used_grant_kb     
    , qs.min_used_grant_kb      
    , qs.max_used_grant_kb      
    , qs.total_ideal_grant_kb   
    , qs.last_ideal_grant_kb    
    , qs.min_ideal_grant_kb     
    , qs.max_ideal_grant_kb     
    , qs.total_reserved_threads 
    , qs.last_reserved_threads  
    , qs.min_reserved_threads   
    , qs.max_reserved_threads   
    , qs.total_used_threads     
    , qs.last_used_threads      
    , qs.min_used_threads       
    , qs.max_used_threads  

    , QS.PLAN_HANDLE
    , REPLACE(REPLACE(REPLACE(     SUBSTRING(QT.TEXT,QS.STATEMENT_START_OFFSET/2 +1, 
               (CASE WHEN QS.STATEMENT_END_OFFSET = -1 
                     THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2 
                     ELSE QS.STATEMENT_END_OFFSET END -
                          QS.STATEMENT_START_OFFSET
               )/2
           ), CHAR(10), '' ''), CHAR(13), '' ''), CHAR(9) , '' '') AS QUERY_TEXT
 FROM #OBJECT_STATS2 X 
 --JOIN #STATEMENT_LEVEL SL ON SL.PLAN_HANDLE = X.PLAN_HANDLE
 left JOIN SYS.DM_EXEC_QUERY_STATS QS ON QS.PLAN_HANDLE = X.PLAN_HANDLE-- AND SL.sql_handle = QS.sql_handle AND SL.statement_start_offset = QS.statement_start_offset AND QS.statement_end_offset = SL.statement_end_offset
 outer APPLY SYS.DM_EXEC_SQL_TEXT(QS.SQL_HANDLE) AS QT 
 WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
 ORDER BY object_NAME, PLAN_HANDLE, ROW_NUM'
                    
                  end
               else
                  begin
                     set @SQL_TEXT = '

 SELECT ''OBJECT STATEMENT-LEVEL OUTPUT'' AS OUTPUT_TYPE
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
           ), CHAR(10), '' ''), CHAR(13), '' ''), CHAR(9) , '' '') AS QUERY_TEXT
 FROM #OBJECT_STATS2 X 
 --JOIN #STATEMENT_LEVEL SL ON SL.PLAN_HANDLE = X.PLAN_HANDLE
 left JOIN SYS.DM_EXEC_QUERY_STATS QS ON QS.PLAN_HANDLE = X.PLAN_HANDLE-- AND SL.sql_handle = QS.sql_handle AND SL.statement_start_offset = QS.statement_start_offset AND QS.statement_end_offset = SL.statement_end_offset
 outer APPLY SYS.DM_EXEC_SQL_TEXT(QS.SQL_HANDLE) AS QT 
 WHERE TOP_N_RANKING_COUNT > 0 OR @APPLY_NO_TOP_FILTERS = 1
 ORDER BY object_NAME, PLAN_HANDLE, ROW_NUM'

                  end

            set @SQL_TEXT = REPLACE(@sql_text, '@APPLY_NO_TOP_FILTERS', @APPLY_NO_TOP_FILTERS)
            print @sql_text
            exec (@sql_text)
                     END --IF @SHOW_OBJECT_STATEMENT_DETAILS = 1'

      IF @SHOW_AD_HOC = 1
         BEGIN
            IF OBJECT_ID(N'TEMPDB..#ALL_QUERY_STATS_ENTRIES') is not null DROP TABLE #ALL_QUERY_STATS_ENTRIES

                  SELECT  QUERY_HASH
                  , QUERY_PLAN_HASH
				  , max(plan_generation_num) as max_plan_generation_num
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
	select *
	from (
      SELECT
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
	   , row_number() over(partition by q.query_hash, q.query_plan_hash order by getdate()) as row_num
         from #AD_HOC_RANK_FILTERED q
         left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handlE
         and 		 qs.query_hash = q.query_hash
         and qs.query_plan_hash = q.query_plan_hash
		 and qs.plan_generation_num = q.max_plan_generation_num
          outer APPLY SYS.DM_EXEC_SQL_TEXT(Q.SAMPLE_SQL_HANDLE) AS QT 
          outer APPLY SYS.DM_EXEC_TEXT_QUERY_PLAN(SAMPLE_PLAN_HANDLE, 0, -1) Y ) tbl
		  where row_num = 1
         ORDER BY query_hash, query_plan_hash
   end -- IF @INCLUDE_AD_HOC_QUERY_PLANS = 1


 

   IF @INCLUDE_AD_HOC_QUERY_PLANS = 0
      BEGIN  
	  select * 
	  from (
         SELECT
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
		  , row_number() over(partition by q.query_hash, q.query_plan_hash order by getdate()) as row_num
            from #AD_HOC_RANK_FILTERED q
            left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handlE
            and qs.query_hash = q.query_hash
            and qs.query_plan_hash = q.query_plan_hash
             outer APPLY SYS.DM_EXEC_SQL_TEXT(Q.SAMPLE_SQL_HANDLE) AS QT ) tbl
		  where row_num = 1
         ORDER BY query_hash, query_plan_hash
      end --IF @INCLUDE_AD_HOC_QUERY_PLANS = 0
   end --IF @SHOW_AD_HOC = 0  

end
--end
