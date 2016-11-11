set nocount on

/* SPIN LOCK INFORMATION

   GRAIN: PER spin lock type, per time interval.

   Because dbcc sqlperf ('spinlockstats') is cumulative since restart, data have to be collected at two points in 
   time and differences calculated.

Parameters:
   |-- DATA COLLECTION TYPE:
      |--@show_current_snapshot:  NO LOOPING.  Dump of dbcc sqlperf ('spinlockstats') with no_infomsgs
         and/or
      |--@calc_interval_diffs:       LOOPING.  Compares data for current loop against prior loop and stores diffs.
         |--@loop_count
         |--@loop_interval_seconds

   |-- FILTERS
      |-- @TOP_N                       Return the top 20 wait types by collisions.  for @calc_interval_diffs, based on sum across all loops
                                       Applied AFTER the next two filters.
      |-- @only_show_changes.          Set to 1 to exclude values where min(calc_value) = max(calc_value).
                                       Since historical snapshot is single data set, @only_show_changes does not apply.
      |-- @only_show_nonzero.          Subset of @only_show_changes.  filters out spinlock types with sum(spins) = 0

*/
-------------------------------------------------------------------
/* DATA COLLECTION PARAMETERS*/

DECLARE @SHOW_CURRENT_SNAPSHOT      BIT         =  1 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.
DECLARE @CALC_INTERVAL_DIFFS        BIT         =  1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @LOOP_COUNT                 INT         =  180 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS      INT         =  60
                                    
/* FILTER PARAMETERS*/  
DECLARE @TOP_N                      INT         =  10 -- RETURN TOP N BY WAIT_TIME_MS descending.  applied after next two filters
DECLARE @ONLY_SHOW_CHANGES          BIT         =  0 -- THERE ARE A LOT OF COUNTERS.  SET TO 1 TO EXCLUDE VALUES WHERE MIN(CALC_VALUE) = MAX(CALC_VALUE)
DECLARE @ONLY_SHOW_NONZERO          BIT         =  1 -- SUBSET OF LOGIC ABOVE.  SOMETIMES YOU WANT TO SEE NON-CHANGING, NON-ZERO VALUES.
DECLARE @EXCLUDE_USELESS_WAIT_TYPES BIT         =  1 -- LIST OF WAIT STATS THAT FIRE TO POLL FOR WORK, ETC.

----------------------------------------------------------------------------
/* PREP WORK*/

declare @total_time int= @loop_count * @LOOP_INTERVAL_SECONDS

select 'Results in ' + cast(@total_time as varchar) + ' seconds.  Check messages tab for progress.'

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin prep work', 10, 1) with nowait

DECLARE @NOW               DATETIME = GETDATE()   --MAKE SURE ALL RECORDS IN THE BATCH ARE INSERTED WITH SAME VALUE
DECLARE @SERVER_INSTANCE    SYSNAME = CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME)
DECLARE @SERVER_START_TIME DATETIME = (SELECT SQLSERVER_START_TIME FROM SYS.DM_OS_SYS_INFO)
DECLARE @DURATION    decimal(34, 3) = DATEDIFF(ms, @SERVER_START_TIME, @NOW)/1000.0
DECLARE @PRIOR_DATETIME    DATETIME = NULL

DECLARE @BATCH_COUNTER INT = 1
DECLARE @ORIGINAL_LOOP_VALUE INT = @LOOP_COUNT

IF OBJECT_ID(N'TEMPDB..#CURRENT') IS NOT NULL DROP TABLE #CURRENT
CREATE TABLE #CURRENT
(
    -- BATCH_DATETIME                 datetime
    --, DURATION_SEC                   decimal(34, 3)
    --, SERVER_INSTANCE                nvarchar(128)
    --, SERVER_START_DATETIME          datetime
    --, 
      spinlock_name                  varchar(50)
    , collisions                     numeric(18,0)
    , spins                          numeric(18,0)
    , spins_per_collision            float(53)
    , sleep_time_ms                  numeric(18,0)
    , backoffs                       numeric(18,0)
) 

IF OBJECT_ID(N'TEMPDB..#PRIOR') IS NOT NULL DROP TABLE #PRIOR
CREATE TABLE #PRIOR
(     PRIOR_ID                       int identity(1, 1)
    , BATCH_ID                       int
    , BATCH_DATETIME                 datetime
    , DURATION_SEC                   decimal(34, 3)
    , SERVER_INSTANCE                nvarchar(128)
    , SERVER_START_DATETIME          datetime
    , spinlock_name                  varchar(50)
    , collisions                     numeric(18,0)
    , spins                          numeric(18,0)
    , spins_per_collision            float(53)
    , sleep_time_ms                  numeric(18,0)
    , backoffs                       numeric(18,0)
) 
IF OBJECT_ID(N'TEMPDB..#HISTORY') IS NOT NULL DROP TABLE #HISTORY
CREATE TABLE #HISTORY
(     HISTORY_ID                     int identity (1, 1)
    , BATCH_ID                       int
    , BATCH_DATETIME                 datetime
    , DURATION_SEC                   decimal(34, 3)
    , SERVER_INSTANCE                nvarchar(128)
    , SERVER_START_DATETIME          datetime
    , spinlock_name                  varchar(50)
    , collisions                     numeric(18,0)
    , spins                          numeric(18,0)
    , spins_per_collision            float(53)
    , sleep_time_ms                  numeric(18,0)
    , backoffs                       numeric(18,0)

) 


IF OBJECT_ID(N'TEMPDB..#aggregate') IS NOT NULL DROP TABLE #aggregate
 
CREATE TABLE #aggregate
(
      output_type         varchar(50)
    , batches             int
    , spinlock_name       varchar(50)
    , filter_status       varchar(19)
    , total_spins         numeric(38,0)
    , avg_spins           numeric(38,6)
    , min_spins           numeric(18,0)
    , max_spins           numeric(18,0)
    , total_collisions    numeric(38,0)
    , avg_collisions      numeric(38,6)
    , min_collisions      numeric(18,0)
    , max_collisions      numeric(18,0)
    , spins_per_collision decimal(38,2)
    , total_sleep_time_ms numeric(38,0)
    , avg_sleep_time_ms   numeric(38,6)
    , min_sleep_time_ms   numeric(18,0)
    , max_sleep_time_ms   numeric(18,0)
    , total_backoffs      numeric(38,0)
    , avg_backoffs        numeric(38,6)
    , min_backoffs        numeric(18,0)
    , max_backoffs        numeric(18,0)
)

IF OBJECT_ID(N'TEMPDB..#aggregate_filtered') IS NOT NULL DROP TABLE #aggregate_filtered
 
CREATE TABLE #aggregate_filtered
(
      output_type         varchar(50)
    , batches             int
    , spinlock_name       varchar(50)
    , filter_status       varchar(19)
    , total_spins         numeric(38,0)
    , avg_spins           numeric(38,6)
    , min_spins           numeric(18,0)
    , max_spins           numeric(18,0)
    , total_collisions    numeric(38,0)
    , avg_collisions      numeric(38,6)
    , min_collisions      numeric(18,0)
    , max_collisions      numeric(18,0)
    , spins_per_collision decimal(38,2)
    , total_sleep_time_ms numeric(38,0)
    , avg_sleep_time_ms   numeric(38,6)
    , min_sleep_time_ms   numeric(18,0)
    , max_sleep_time_ms   numeric(18,0)
    , total_backoffs      numeric(38,0)
    , avg_backoffs        numeric(38,6)
    , min_backoffs        numeric(18,0)
    , max_backoffs        numeric(18,0)
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
        ([spinlock_name], collisions, spins, [spins_per_collision], [sleep_time_ms], backoffs) 
               execute ('dbcc sqlperf (''spinlockstats'') with no_infomsgs')


      raiserror('   |--- End   Insert into #CURRENT ', 10, 1) with nowait

      IF @SHOW_CURRENT_SNAPSHOT = 1 and @BATCH_COUNTER = 1
         BEGIN
            SELECT 
              'Spinlocks - Historical'  as OUTPUT_TYPE
            , @NOW                      AS BATCH_DATETIME
            , @DURATION                 AS DURATION_SEC 
            , @SERVER_INSTANCE          AS SERVER_INSTANCE 
            , @SERVER_START_TIME        AS SERVER_START_DATETIME
            , *
            FROM #CURRENT
            where @ONLY_SHOW_NONZERO = 0 or ( @ONLY_SHOW_NONZERO = 1 and spins <> 0)
            ORDER BY spins desc

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
               SELECT @BATCH_COUNTER
                     , @NOW                      AS BATCH_DATETIME
                     , @DURATION                 AS DURATION_SEC 
                     , @SERVER_INSTANCE          AS SERVER_INSTANCE 
                     , @SERVER_START_TIME        AS SERVER_START_DATETIME
                     , * 
               FROM #CURRENT
       
            INSERT INTO #HISTORY
               SELECT @BATCH_COUNTER
                     , @NOW                      AS BATCH_DATETIME
                     , @DURATION                 AS DURATION_SEC 
                     , @SERVER_INSTANCE          AS SERVER_INSTANCE 
                     , @SERVER_START_TIME        AS SERVER_START_DATETIME
                     , * 
               FROM #CURRENT

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
                     , @NOW                      AS BATCH_DATETIME
                     , @DURATION                 AS DURATION_SEC 
                     , @SERVER_INSTANCE          AS SERVER_INSTANCE 
                     , @SERVER_START_TIME        AS SERVER_START_DATETIME
                     , c.spinlock_name    
                     , coalesce(c.collisions, 0)    - coalesce(P.collisions , 0)    AS collisions
                     , coalesce(C.spins , 0)        - coalesce(P.spins , 0)         AS spins
                     , null  AS spins_per_collision
                     , coalesce(c.sleep_time_ms, 0) - coalesce(P.sleep_time_ms , 0) AS sleep_time_ms
                     , coalesce(C.backoffs , 0)     - coalesce(P.backoffs , 0)      AS backoffs
               FROM #CURRENT    C
               FULL JOIN #PRIOR P ON P.spinlock_name = C.spinlock_name

            truncate table #prior

            INSERT INTO #PRIOR
               SELECT @BATCH_COUNTER
                     , @NOW                      AS BATCH_DATETIME
                     , @DURATION                 AS DURATION_SEC 
                     , @SERVER_INSTANCE          AS SERVER_INSTANCE 
                     , @SERVER_START_TIME        AS SERVER_START_DATETIME
               , * FROM #CURRENT
       



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
   select 'Spinlocks - Loops Aggregated' as output_type
   , count(*) as batches
   , spinlock_name
   , case when sum(spins) = 0 then 'No Spins'
          when min(spins) = max(spins) then 'No Changes in Spins' else '' end as filter_status
   , sum(spins) as total_spins
   , avg(spins) as avg_spins
   , min(spins) as min_spins
   , max(spins) as max_spins

   , sum(collisions) as total_collisions
   , avg(collisions) as avg_collisions
   , min(collisions) as min_collisions
   , max(collisions) as max_collisions

   , cast(case when sum(spins) = 0 then 0
          else sum(collisions) * 1.0 / sum(spins) end as decimal(38, 2)) as spins_per_collision

   , sum(sleep_time_ms) as total_sleep_time_ms
   , avg(sleep_time_ms) as avg_sleep_time_ms
   , min(sleep_time_ms) as min_sleep_time_ms
   , max(sleep_time_ms) as max_sleep_time_ms

   , sum(backoffs) as total_backoffs
   , avg(backoffs) as avg_backoffs
   , min(backoffs) as min_backoffs
   , max(backoffs) as max_backoffs

   from #HISTORY
   where batch_id <> 1
   group by spinlock_name




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
   order by total_spins desc

select *
from #aggregate_filtered
order by total_spins desc

SELECT 'Spinlocks - Loop Details' as output_type
    , h.batch_id
    , h.batch_datetime
    , h.duration_sec
    , h.server_instance
    , h.server_start_datetime
    , h.spinlock_name
    , h.spins
    , h.collisions
    , CAST( case when h.spins = 0 then 0 
                 else h.collisions *1.0/h.spins end as decimal(38, 4)) as avg_collisions_per_spin
    , h.backoffs
    , h.sleep_time_ms
FROM #HISTORY h
join #aggregate_filtered a on a.spinlock_name = h.spinlock_name
where batch_id <> 1
order by spinlock_name, BATCH_DATETIME

if @@ROWCOUNT = 0
   begin
      select 'No spinlocks logged during the collection period met the filter criteria.'
   end


Raiserror('|- End   output from loop', 10, 1) with nowait
Raiserror('|----------------------------------------------------------------', 10, 1) with nowait


