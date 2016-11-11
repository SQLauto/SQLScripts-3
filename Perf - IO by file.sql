

/****** Object:  StoredProcedure [dbo].[STATS_InsIOPerformance_to_Perfprod]    Script Date: 4/22/2015 4:43:24 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

set nocount on
/* BATCH READ AND WRITE PERFORMANCE - KBs, latencies, iops
   GRAIN: PER db, per file_id, per time interval.

   Because sys.dm_io_virtual_file_stats is cumulative since restart, data have to be collected at two points in 
   time and differences calculated.

Parameters:
   |-- DATA COLLECTION TYPE:
      |--@show_historical_snapshot:  NO LOOPING.  Dump of sys.dm_io_virtual_file_stats.  counts as first batch.
         and/or
      |--@calc_interval_diffs:       LOOPING.  Compares data for current loop against prior loop and stores diffs.
         |--@loop_count
         |--@loop_interval_seconds

   |-- FILTERS
      |-- @only_show_changes.          Set to 1 to exclude values where min(calc_value) = max(calc_value).
                                       Since historical snapshot is single data set, @only_show_changes does not apply.
      |-- @only_show_nonzero.          Subset of @only_show_changes.  sometimes you want to see non-changing, non-zero values.

*/

DECLARE @SHOW_HISTORICAL_SNAPSHOT    BIT         =  1 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.

DECLARE @CALC_INTERVAL_DIFFS         BIT         =  1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @LOOP_COUNT                  INT         =  10 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS       INT         =  1
                                    
/* FILTER PARAMETERS*/  
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

declare @total_time int= @loop_count * @LOOP_INTERVAL_SECONDS

select 'Results in ' + cast(@total_time as varchar) + ' seconds.  Check messages tab for progress.'

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin prep work', 10, 1) with nowait

DECLARE @NOW               DATETIME = GETDATE()   --MAKE SURE ALL RECORDS IN THE BATCH ARE INSERTED WITH SAME VALUE
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
         FROM SYS.DM_IO_VIRTUAL_FILE_STATS(NULL,NULL) IO
            JOIN sys.databases                         D ON D.database_id = IO.database_id
            JOIN SYS.MASTER_FILES                     MF ON MF.FILE_ID = IO.FILE_ID
                                                            AND MF.DATABASE_ID = IO.DATABASE_ID

      raiserror('   |--- End   Insert into #CURRENT ', 10, 1) with nowait

      IF @SHOW_historical_SNAPSHOT = 1 and @BATCH_COUNTER = 1
         BEGIN
            SELECT 'IO - History' as OUTPUT_TYPE
            , @BATCH_COUNTER AS BATCH_ID
            , *
            , CASE WHEN NUM_OF_READS = 0 then 'n/a - 0 reads'                  
                   else CASE WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '1- EXCELLENT'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_2_GOOD_MS       THEN '2- GOOD'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_3_MARGINAL_MS   THEN '3- MARGINAL'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_4_PROBLEM_MS    THEN '4- PROBLEM'  
                             WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '5- REAL PROBLEM'  
                             ELSE                                                                                   '6- CRITICAL'                               
                        END                                                   
               END AS READ_STALL_CATEGORY                                     
            , CASE WHEN NUM_OF_WRITES = 0 then 'n/a - 0 writes'               
                   else CASE WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '1- EXCELLENT'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_2_GOOD_MS       THEN '2- GOOD'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '3- MARGINAL'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '4- PROBLEM'  
                             WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '5- REAL PROBLEM'  
                             ELSE                                                                                    '6- CRITICAL'    
                        END
               END AS WRITE_STALL_CATEGORY

            FROM #CURRENT

            WHERE ((@EXCLUDE_FILES_WITH_0_READS = 1 AND NUM_OF_READS > 0 OR @EXCLUDE_FILES_WITH_0_READS = 0)
                  AND 
                  (@EXCLUDE_FILES_WITH_0_WRITES = 1 AND NUM_OF_WRITES > 0 OR @EXCLUDE_FILES_WITH_0_WRITES = 0))
            ORDER BY DATABASE_NAME, FILE_TYPE, FILE_NAME

        raiserror('   |--------- Begin IF @CALC_INTERVAL_DIFFS = 0  ', 10, 1) with nowait

         IF @CALC_INTERVAL_DIFFS = 0 
            BEGIN
               RETURN
            END --IF @CALC_INTERVAL_DIFFS = 0 

        raiserror('   |--------- End   IF @CALC_INTERVAL_DIFFS = 0   ', 10, 1) with nowait

         END --IF @SHOW_CURRENT_SNAPSHOT = 1

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

      waitfor delay @LOOP_INTERVAL_SECONDS

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



/* only show wait stats with any values in time monitored.*/

--DECLARE @EXCLUDE_FILES_WITH_0_READS  BIT         =  1
--DECLARE @EXCLUDE_FILES_WITH_0_WRITES BIT         =  1


insert into #aggregate_filtered
   select * 
   from #aggregate  a
            WHERE ((@EXCLUDE_FILES_WITH_0_READS = 1 AND total_reads > 0 OR @EXCLUDE_FILES_WITH_0_READS = 0)
                  or 
                  (@EXCLUDE_FILES_WITH_0_WRITES = 1 AND total_WRITES > 0 OR @EXCLUDE_FILES_WITH_0_WRITES = 0))


--DECLARE @LATENCY_CAT_1_EXCELLENT_MS  DECIMAL(10, 1) = 5
--DECLARE @LATENCY_CAT_2_GOOD_MS       DECIMAL(10, 1) = 10
--DECLARE @LATENCY_CAT_3_MARGINAL_MS   DECIMAL(10, 1) = 20
--DECLARE @LATENCY_CAT_4_PROBLEM_MS    DECIMAL(10, 1) = 30
--DECLARE @LATENCY_CAT_5_REAL_ISSUE_MS DECIMAL(10, 1) = 50

IF OBJECT_ID(N'TEMPDB..#history_categories') IS NOT NULL DROP TABLE #history_categories
 
SELECT  h.batch_id
      , h.DATABASE_ID
      , h.FILE_ID
      , CASE WHEN NUM_OF_READS = 0 then 'n/a - 0 reads' 
             else CASE WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                       WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                       WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                       WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                       WHEN coalesce(AVG_READ_LATENCY_PER_READ_MS, 0)    <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
                       ELSE '5- CRITICAL'
                  END
         END AS READ_STALL_CATEGORY
      , CASE WHEN NUM_OF_WRITES = 0 then 'n/a - 0 writes' 
             else CASE WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_1_EXCELLENT_MS  THEN '0- EXCELLENT'  
                       WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_2_GOOD_MS       THEN '1- GOOD'  
                       WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_3_MARGINAL_MS   THEN '2- MARGINAL'  
                       WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_4_PROBLEM_MS    THEN '3- PROBLEM'  
                       WHEN coalesce(AVG_WRITE_LATENCY_PER_WRITE_MS, 0)   <= @LATENCY_CAT_5_REAL_ISSUE_MS THEN '4- REAL PROBLEM'  
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
                                                      
SELECT                                         
      'IO - Loop Details' as OUTPUT_TYPE      
    , h.BATCH_ID
    , h.BATCH_DATETIME
    , h.DURATION_SEC
    , h.SERVER_INSTANCE
    , h.SERVER_START_DATETIME
    , h.DATABASE_ID
    , h.DATABASE_NAME
    , h.FILE_ID
    , h.FILE_NAME
    , h.FILE_TYPE
    , hc.READ_STALL_CATEGORY
    , hc.WRITE_STALL_CATEGORY
    , h.NUM_OF_READS
    , h.KB_READ
    , h.AVG_KB_READ_PER_READ
    , h.AVG_KB_READ_PER_SECOND
    , h.AVG_READ_LATENCY_PER_READ_MS
    , h.AVG_READ_LATENCY_PER_KB_MS
    , h.READ_LATENCY_TOTAL_MS
    , h.NUM_OF_WRITES
    , h.KB_WRITTEN
    , h.AVG_KB_WRITTEN_PER_READ
    , h.AVG_KB_WRITTEN_PER_SECOND
    , h.AVG_WRITE_LATENCY_PER_WRITE_MS
    , h.AVG_WRITE_LATENCY_PER_KB_MS
    , h.WRITE_LATENCY_TOTAL_MS
from #HISTORY h
join #history_categories hc on hc.DATABASE_ID = h.DATABASE_ID
                        and hc.FILE_ID = h.FILE_ID
                        and hc.BATCH_ID = h.BATCH_ID
where h.BATCH_ID > 1
--WHERE NUM_OF_READS + NUM_OF_WRITES > 0
order by DATABASE_NAME, FILE_ID, batch_datetime

Raiserror('|- End   output from loop', 10, 1) with nowait
Raiserror('|----------------------------------------------------------------', 10, 1) with nowait

