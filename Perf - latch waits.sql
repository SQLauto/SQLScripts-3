set nocount on
/* latch wait information
   grain: per latch type, per time interval.

   because sys.dm_os_latch_stats is cumulative since restart, data have to be collected at two points in 
   time and differences calculated.

Results are stored in a temp table and returned after the specified number of loops.

Parameters:
   |-- DATA COLLECTION TYPE:
      |--@show_current_snapshot:  NO LOOPING.  Dump of sys.dm_os_latch_stats
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
*/
---------------------------------------------------------------------------
/* DATA COLLECTION PARAMETERS*/

DECLARE @SHOW_CURRENT_SNAPSHOT      BIT         =  1 -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.
DECLARE @CALC_INTERVAL_DIFFS        BIT         =  1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @LOOP_COUNT                 INT         =  180 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS      INT         =  60
                                    
/* FILTER PARAMETERS*/  
DECLARE @TOP_N                      INT         =  10 -- RETURN TOP N BY WAIT_TIME_MS descending.  applied after next two filters
DECLARE @ONLY_SHOW_CHANGES          BIT         =  0 -- THERE ARE A LOT OF COUNTERS.  SET TO 1 TO EXCLUDE VALUES WHERE MIN(CALC_VALUE) = MAX(CALC_VALUE)
DECLARE @ONLY_SHOW_NONZERO          BIT         = 1 -- SUBSET OF LOGIC ABOVE.  SOMETIMES YOU WANT TO SEE NON-CHANGING, NON-ZERO VALUES.

----------------------------------------------------------------------------
/* prep work*/

declare @total_time int= @loop_count * @loop_interval_seconds

select 'results in ' + cast(@total_time as varchar) + ' seconds.  check messages tab for progress.'

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- begin prep work', 10, 1) with nowait

declare @now               datetime = getdate()   --make sure all records in the batch are inserted with same value
declare @server_instance    sysname = cast(serverproperty('servername') as sysname)
declare @server_start_time datetime = (select sqlserver_start_time from sys.dm_os_sys_info)
declare @duration    decimal(34, 3) = datediff(second, @server_start_time, @now)
declare @prior_datetime    datetime = null

declare @batch_counter int = 1
declare @original_loop_value int = @loop_count

if object_id(N'tempdb..#current') is not null drop table #current
create table #current
(     batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , latch_class                    nvarchar(60)
    , waiting_requests_count         bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
) 

if object_id(N'tempdb..#prior') is not null drop table #prior
create table #prior
(     prior_id                       int identity(1, 1)
    , batch_id                       int
    , batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , latch_class                    nvarchar(60)
    , waiting_requests_count         bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
) 
if object_id(N'tempdb..#history') is not null drop table #history
create table #history
(     history_id                     int identity (1, 1)
    , batch_id                       int
    , batch_datetime                 datetime
    , duration_sec                   decimal(34, 3)
    , server_instance                nvarchar(128)
    , server_start_datetime          datetime
    , latch_class                    nvarchar(60)
    , waiting_requests_count         bigint
    , wait_time_ms                   bigint
    , max_wait_time_ms               bigint
) 

IF OBJECT_ID(N'TEMPDB..#aggregate') IS NOT NULL DROP TABLE #aggregate
 
CREATE TABLE #aggregate
(
      output_type                  varchar(50)
    , batches                      int
    , latch_class                  nvarchar(60)
    , filter_status                varchar(19)
    , total_waiting_requests_count bigint
    , avg_waiting_requests_count   bigint
    , min_waiting_requests_count   bigint
    , max_waiting_requests_count   bigint
    , wait_time_ms                 bigint
    , avg_wait_time_ms             bigint
    , min_wait_time_ms             bigint
    , max_wait_time_ms             bigint
    , avg_wait_ms_per_wait         decimal(38,2)
)

IF OBJECT_ID(N'TEMPDB..#aggregate_filtered') IS NOT NULL DROP TABLE #aggregate_filtered
 
CREATE TABLE #aggregate_filtered
(
      output_type                  varchar(50)
    , batches                      int
    , latch_class                  nvarchar(60)
    , filter_status                varchar(19)
    , total_waiting_requests_count bigint
    , avg_waiting_requests_count   bigint
    , min_waiting_requests_count   bigint
    , max_waiting_requests_count   bigint
    , wait_time_ms                 bigint
    , avg_wait_time_ms             bigint
    , min_wait_time_ms             bigint
    , max_wait_time_ms             bigint
    , avg_wait_ms_per_wait         decimal(38,2)
)

raiserror('|- end   prep work', 10, 1) with nowait
raiserror('|----------------------------------------------------------------', 10, 1) with nowait

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- begin loop', 10, 1) with nowait
while @loop_count >= 1
   begin
      print @loop_count

      set @now = getdate()

      raiserror('   |--- begin insert into #current ', 10, 1) with nowait

      truncate table #current

      insert into #current
         select  
              @now                      as batch_datetime
            , @duration                 as duration_sec 
            , @server_instance          as server_instance 
            , @server_start_time        as server_start_datetime
            , ls.latch_class           
            , ls.waiting_requests_count
            , ls.wait_time_ms          
            , ls.max_wait_time_ms      
         from sys.dm_os_latch_stats  ls 


      raiserror('   |--- end   insert into #current ', 10, 1) with nowait

      if @show_current_snapshot = 1 and @batch_counter = 1
         begin
            SELECT top (@TOP_N)
               'Latch Waits - Historical' as OUTPUT_TYPE
               , @BATCH_COUNTER AS BATCH_ID
               , *
            from #current
            where @ONLY_SHOW_NONZERO = 0 or ( @ONLY_SHOW_NONZERO = 1 and waiting_requests_count <> 0)
            order by waiting_requests_count desc

        raiserror('   |--------- begin if @calc_interval_diffs = 0  ', 10, 1) with nowait

         if @calc_interval_diffs = 0 
            begin
               return
            end --if @calc_interval_diffs = 0 

        raiserror('   |--------- end   if @calc_interval_diffs = 0   ', 10, 1) with nowait

         end --if @show_current_snapshot = 1

        ----------------------------------------------------------------------------
         /* a.  if first collection, 
                   i.  load staging table 
                   ii.  load history table.
         */

      raiserror('   |--- begin if @original_loop_value = @loop_count  ', 10, 1) with nowait

      if @original_loop_value = @loop_count 
         begin
            insert into #prior
               select @batch_counter, * from #current
       
            insert into #history
               select @batch_counter, * from #current

            set @prior_datetime = @now
            set @now = getdate()

            --select 'a', @original_loop_value, @loop_count,  @batch_counter,  convert(varchar(100), @now, 120), convert(varchar(100), @prior_datetime, 120)
            raiserror('   |--- end   if @original_loop_value = @loop_count ', 10, 1) with nowait

         end --if @original_loop_value = @loop_count 


         ----------------------------------------------------------------------------
         /*
             b.  if subsequent collection
                   i.  compare values from current collection and staging (prior) collection.
                   ii.  load calculated values into history
                   iii.  load current collection into staging
         */
     
      else if @original_loop_value <> @loop_count 
         begin
            raiserror('   |--- begin if @original_loop_value <> @loop_count  ', 10, 1) with nowait

            set @duration = datediff(ms, @prior_datetime, @now)/1000.0

            insert into #history
               select @batch_counter
               , c.batch_datetime
               , @duration              
               , c.server_instance
               , c.server_start_datetime
               , c.latch_class           
               , coalesce(c.waiting_requests_count, 0) - coalesce(p.waiting_requests_count , 0) as waiting_requests_count
               , coalesce(c.wait_time_ms , 0)     - coalesce(p.wait_time_ms , 0)                as wait_time_ms
               , case when coalesce(c.max_wait_time_ms, 0) > coalesce(p.max_wait_time_ms, 0) 
                      then coalesce(c.max_wait_time_ms, 0) 
                      else coalesce(p.max_wait_time_ms, 0) end as max_wait_time_ms
               from #current    c
               full join #prior p on p.latch_class = c.latch_class

            truncate table #prior

            insert into #prior
               select @batch_counter, * from #current

         end --if @original_loop_value <> @loop_count 

      raiserror('   |--- end   if @original_loop_value <> @loop_count ', 10, 1) with nowait

      set @loop_count = @loop_count - 1
      set @batch_counter = @batch_counter + 1
      set  @prior_datetime = @now 

      waitfor delay @loop_interval_seconds

   end --while @counter <= @loop_count
raiserror('|- end   loop', 10, 1) with nowait
raiserror('|----------------------------------------------------------------', 10, 1) with nowait

----------------------------------------------------------------------------------------------------

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- begin output from loop', 10, 1) with nowait

/* get aggregates - used for reporting and for filtering details*/
insert into #aggregate
   select 'Latch Waits - Loops Aggregated' as output_type
   , count(*)                 as batches
   , latch_class
   , case when sum(waiting_requests_count) = 0 then 'No Waits'
          when min(waiting_requests_count) = max(waiting_requests_count) then 'No Changes in Waits' else '' end as filter_status
   , sum(waiting_requests_count) as total_waiting_requests_count
   , avg(waiting_requests_count) as avg_waiting_requests_count
   , min(waiting_requests_count) as min_waiting_requests_count
   , max(waiting_requests_count) as max_waiting_requests_count

   , sum(wait_time_ms       ) as wait_time_ms
   , avg(wait_time_ms       ) as avg_wait_time_ms
   , min(wait_time_ms       ) as min_wait_time_ms
   , max(wait_time_ms       ) as max_wait_time_ms

   , cast(case when sum(waiting_requests_count) = 0 then 0
          else sum(wait_time_ms       ) * 1.0 / sum(waiting_requests_count) end as decimal(38, 2)) as avg_wait_ms_per_wait
   from #HISTORY
   where batch_id <> 1
   group by latch_class

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

SELECT 'Latch Waits - Loop Details' as output_type
    , h.batch_id
    , h.batch_datetime
    , h.duration_sec
    , h.server_instance
    , h.server_start_datetime
    , h.latch_class
    , h.waiting_requests_count
    , h.wait_time_ms
    , CAST( case when h.waiting_requests_count = 0 then 0 
                 else h.wait_time_ms *1.0/h.waiting_requests_count end as decimal(38, 4)) as avg_ms_per_wait
    , h.max_wait_time_ms
FROM #HISTORY h
join #aggregate_filtered a on a.latch_class = h.latch_class
where batch_id <> 1
order by latch_class, BATCH_DATETIME


if @@rowcount = 0
   begin
      select 'Any latches logged during the collection period did not meet filter criteria.'
   end

raiserror('|- end   output from loop', 10, 1) with nowait
raiserror('|----------------------------------------------------------------', 10, 1) with nowait

