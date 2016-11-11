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
DECLARE @LOOP_COUNT                 INT         =  3 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS      INT         =  1
                                    
/* FILTER PARAMETERS*/  

----------------------------------------------------------------------------
/* prep work*/

declare @total_time int= @loop_count * @loop_interval_seconds

select 'Results in ' + cast(@total_time as varchar) + ' seconds.  check messages tab for progress.'

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
(     batch_datetime             datetime
    , duration_sec               decimal(34, 3)
    , server_instance            nvarchar(128)
    , server_start_datetime      datetime
    , parent_node_id             int
    , scheduler_id               int
    , cpu_id                     int
    , scheduler_role             nvarchar(60)
    , is_online                  bit
    , is_idle                    bit
    , preemptive_switches_count  int
    , context_switches_count     int
    , idle_switches_count        int
    , yield_count                int
    , current_tasks_count        int
    , runnable_tasks_count       int
    , current_workers_count      int
    , active_workers_count       int
    , work_queue_count           bigint
    , pending_disk_io_count      int
    , load_factor                int
    , last_timer_activity        bigint
    , failed_to_create_worker    bit
) 

if object_id(N'tempdb..#prior') is not null drop table #prior
create table #prior
(     prior_id                   int identity(1, 1)
    , batch_id                   int
    , batch_datetime             datetime
    , duration_sec               decimal(34, 3)
    , server_instance            nvarchar(128)
    , server_start_datetime      datetime
    , parent_node_id             int
    , scheduler_id               int
    , cpu_id                     int
    , scheduler_role             nvarchar(60)
    , is_online                  bit
    , is_idle                    bit
    , preemptive_switches_count  int
    , context_switches_count     int
    , idle_switches_count        int
    , yield_count                int
    , current_tasks_count        int
    , runnable_tasks_count       int
    , current_workers_count      int
    , active_workers_count       int
    , work_queue_count           bigint
    , pending_disk_io_count      int
    , load_factor                int
    , last_timer_activity        bigint
    , failed_to_create_worker    bit
) 
if object_id(N'tempdb..#history') is not null drop table #history
create table #history
(     history_id                 int identity (1, 1)
    , batch_id                   int
    , batch_datetime             datetime
    , duration_sec               decimal(34, 3)
    , server_instance            nvarchar(128)
    , server_start_datetime      datetime
    , parent_node_id             int
    , scheduler_id               int
    , cpu_id                     int
    , scheduler_role             nvarchar(60)
    , is_online                  bit
    , is_idle                    bit
    , preemptive_switches_count  int
    , context_switches_count     int
    , idle_switches_count        int
    , yield_count                int
    , current_tasks_count        int
    , runnable_tasks_count       int
    , current_workers_count      int
    , active_workers_count       int
    , work_queue_count           bigint
    , pending_disk_io_count      int
    , load_factor                int
    , last_timer_activity        bigint
    , timer_change               int
    , failed_to_create_worker    bit
) 

IF OBJECT_ID(N'TEMPDB..#aggregate') IS NOT NULL DROP TABLE #aggregate
 
CREATE TABLE #aggregate
(
      output_type                   varchar(33)
    , parent_node_id                int
    , scheduler_id                  int
    , cpu_id                        int
    , scheduler_role                nvarchar(60)
    , is_online                     bit
    , batches                       int
    , duration_sec_total            decimal(38, 3)
    , duration_sec_avg              decimal(38, 3)
    , idle_count                    int
    , yield_count                   int
    , context_switches_count        int
    , idle_switches_count           int
    , preemptive_switches_count     int
    , max_possible_yields           int
    , preemptive_switches_avg       decimal(38, 3)
    , context_switches_avg          decimal(38, 3)
    , idle_switches_avg             decimal(38, 3)
    , yield_avg                     decimal(38, 3)
    , preemptive_switches_min       int
    , context_switches_min          int
    , idle_switches_min             int
    , yield_min                     int
    , preemptive_switches_max       int
    , context_switches_max          int
    , idle_switches_max             int
    , yield_max                     int
    , current_tasks_avg             decimal(38, 3)
    , runnable_tasks_avg            decimal(38, 3)
    , current_workers_avg           decimal(38, 3)
    , active_workers_avg            decimal(38, 3)
    , work_queue_avg                decimal(38, 3)
    , pending_disk_io_avg           decimal(38, 3)
    , load_factor_avg               decimal(38, 3)
    , current_tasks_min             int
    , runnable_tasks_min            int
    , current_workers_min           int
    , active_workers_min            int
    , work_queue_min                bigint
    , pending_disk_io_min           int
    , load_factor_min               int
    , current_tasks_max             int
    , runnable_tasks_max            int
    , current_workers_max           int
    , active_workers_max            int
    , work_queue_max                bigint
    , pending_disk_io_max           int
    , load_factor_max               int
    , last_timer_activity           bigint
    , timer_change_count            int
    , failed_to_create_worker_count int

)

IF OBJECT_ID(N'TEMPDB..#aggregate_filtered') IS NOT NULL DROP TABLE #aggregate_filtered
 
CREATE TABLE #aggregate_filtered
(
      output_type                   varchar(33)
    , parent_node_id                int
    , scheduler_id                  int
    , cpu_id                        int
    , scheduler_role                nvarchar(60)
    , is_online                     bit
    , batches                       int
    , duration_sec_total            decimal(38, 3)
    , duration_sec_avg              decimal(38, 3)
    , idle_count                    int
    , yield_count                   int
    , context_switches_count        int
    , idle_switches_count           int
    , preemptive_switches_count     int
    , max_possible_yields           int
    , preemptive_switches_avg       decimal(38, 3)
    , context_switches_avg          decimal(38, 3)
    , idle_switches_avg             decimal(38, 3)
    , yield_avg                     decimal(38, 3)
    , preemptive_switches_min       int
    , context_switches_min          int
    , idle_switches_min             int
    , yield_min                     int
    , preemptive_switches_max       int
    , context_switches_max          int
    , idle_switches_max             int
    , yield_max                     int
    , current_tasks_avg             decimal(38, 3)
    , runnable_tasks_avg            decimal(38, 3)
    , current_workers_avg           decimal(38, 3)
    , active_workers_avg            decimal(38, 3)
    , work_queue_avg                decimal(38, 3)
    , pending_disk_io_avg           decimal(38, 3)
    , load_factor_avg               decimal(38, 3)
    , current_tasks_min             int
    , runnable_tasks_min            int
    , current_workers_min           int
    , active_workers_min            int
    , work_queue_min                bigint
    , pending_disk_io_min           int
    , load_factor_min               int
    , current_tasks_max             int
    , runnable_tasks_max            int
    , current_workers_max           int
    , active_workers_max            int
    , work_queue_max                bigint
    , pending_disk_io_max           int
    , load_factor_max               int
    , last_timer_activity           bigint
    , timer_change_count            int
    , failed_to_create_worker_count int
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
            , sc.parent_node_id
            , sc.scheduler_id
            , sc.cpu_id
            , case when sc.status like 'hidden%' then 'Hidden - Internal Requests'
                   when sc.status like '%dac%' then 'DAC'
                   when sc.status like 'visible%' then 'Visible - User Requests'
                   else sc.status end as Scheduler_Role
            , sc.is_online
            , sc.is_idle
            , sc.preemptive_switches_count
            , sc.context_switches_count
            , sc.idle_switches_count
            , sc.yield_count
            , sc.current_tasks_count
            , sc.runnable_tasks_count
            , sc.current_workers_count
            , sc.active_workers_count
            , sc.work_queue_count
            , sc.pending_disk_io_count
            , sc.load_factor
            , sc.last_timer_activity
            , sc.failed_to_create_worker
         from sys.dm_os_schedulers  sc 
         


      raiserror('   |--- end   insert into #current ', 10, 1) with nowait

      if @show_current_snapshot = 1 and @batch_counter = 1
         begin
            SELECT
               'CPU Schedulers - Historical' as OUTPUT_TYPE
               , @BATCH_COUNTER AS BATCH_ID
               , *
            from #current
            order by scheduler_role desc, scheduler_id

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
               select @batch_counter
                   ,  batch_datetime
                   , duration_sec
                   , server_instance
                   , server_start_datetime
                   , parent_node_id
                   , scheduler_id
                   , cpu_id
                   , scheduler_role
                   , is_online
                   , is_idle
                   , preemptive_switches_count
                   , context_switches_count
                   , idle_switches_count
                   , yield_count
                   , current_tasks_count
                   , runnable_tasks_count
                   , current_workers_count
                   , active_workers_count
                   , work_queue_count
                   , pending_disk_io_count
                   , load_factor
                   , last_timer_activity
                   , null as timer_change
                   , failed_to_create_worker
                from #current

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
               , c.parent_node_id
               , c.scheduler_id
               , c.cpu_id
               , c.Scheduler_Role
               , c.is_online
               , c.is_idle
               , coalesce(c.preemptive_switches_count, 0) - coalesce(p.preemptive_switches_count, 0) as preemptive_switches_count
               , coalesce(c.context_switches_count   , 0) - coalesce(p.context_switches_count   , 0) as context_switches_count
               , coalesce(c.idle_switches_count      , 0) - coalesce(p.idle_switches_count      , 0) as idle_switches_count
               , coalesce(c.yield_count              , 0) - coalesce(p.yield_count              , 0) as yield_count
               , c.current_tasks_count
               , c.runnable_tasks_count
               , c.current_workers_count
               , c.active_workers_count
               , c.work_queue_count
               , c.pending_disk_io_count
               , c.load_factor
               , c.last_timer_activity
               , case when c.last_timer_activity <> p.last_timer_activity then 1 else 0 end as timer_change
               , c.failed_to_create_worker
               from #current    c
               full join #prior p on p.scheduler_id = c.scheduler_id

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

--/* get aggregates - used for reporting and for filtering details*/

insert into #aggregate
   select 'CPU Schedulers - Loops Aggregated' as output_type
      , parent_node_id            
      , scheduler_id              
      , cpu_id                    
      , scheduler_role            
      , is_online                 
      , count(*)                              as batches
      , sum(duration_sec)                     as duration_sec_total
      , avg(duration_sec)                     as duration_sec_avg

      , sum(cast(is_idle as int))             as idle_count                 
      , sum(yield_count              )        as yield_count 
      , sum(context_switches_count   )        as context_switches_count 
      , sum(idle_switches_count      )        as idle_switches_count 
      , sum(preemptive_switches_count)        as preemptive_switches_count
      , sum(duration_sec)/ 0.004              as max_possible_yields
      , avg(preemptive_switches_count)        as preemptive_switches_avg
      , avg(context_switches_count   )        as context_switches_avg 
      , avg(idle_switches_count      )        as idle_switches_avg 
      , avg(yield_count              )        as yield_avg 
      , min(preemptive_switches_count)        as preemptive_switches_min
      , min(context_switches_count   )        as context_switches_min 
      , min(idle_switches_count      )        as idle_switches_min 
      , min(yield_count              )        as yield_min 
      , max(preemptive_switches_count)        as preemptive_switches_max
      , max(context_switches_count   )        as context_switches_max 
      , max(idle_switches_count      )        as idle_switches_max 
      , max(yield_count              )        as yield_max 
      , avg(current_tasks_count      )        as  current_tasks_avg  
      , avg(runnable_tasks_count     )        as  runnable_tasks_avg 
      , avg(current_workers_count    )        as  current_workers_avg
      , avg(active_workers_count     )        as  active_workers_avg 
      , avg(work_queue_count         )        as  work_queue_avg     
      , avg(pending_disk_io_count    )        as  pending_disk_io_avg
      , avg(load_factor              )        as  load_factor_avg          
      , min(current_tasks_count      )        as  current_tasks_min  
      , min(runnable_tasks_count     )        as  runnable_tasks_min 
      , min(current_workers_count    )        as  current_workers_min
      , min(active_workers_count     )        as  active_workers_min 
      , min(work_queue_count         )        as  work_queue_min     
      , min(pending_disk_io_count    )        as  pending_disk_io_min
      , min(load_factor              )        as  load_factor_min          
      , max(current_tasks_count      )        as  current_tasks_max  
      , max(runnable_tasks_count     )        as  runnable_tasks_max 
      , max(current_workers_count    )        as  current_workers_max
      , max(active_workers_count     )        as  active_workers_max 
      , max(work_queue_count         )        as  work_queue_max     
      , max(pending_disk_io_count    )        as  pending_disk_io_max
      , max(load_factor              )        as  load_factor_max  
      , max(last_timer_activity      )        as  last_timer_activity  
      , sum(timer_change             )        as timer_change_count
      , sum(cast(failed_to_create_worker as int)) as failed_to_create_worker_count 
   from #HISTORY
   where batch_id <> 1
   group by parent_node_id            
          , scheduler_id              
          , cpu_id                    
          , scheduler_role            
          , is_online                 


--/* apply filters on values collected in time monitored.*/


insert into #aggregate_filtered
   select  * 
   from #aggregate  a

select *
from #aggregate_filtered
order by scheduler_role desc, scheduler_id

SELECT 'Latch Waits - Loop Details' as output_type
    , h.*
from #history h
where batch_id > 1
order by BATCH_DATETIME, scheduler_role desc, scheduler_Id


if @@rowcount = 0
   begin
      select 'Any latches logged during the collection period did not meet filter criteria.'
   end

raiserror('|- end   output from loop', 10, 1) with nowait
raiserror('|----------------------------------------------------------------', 10, 1) with nowait

set nocount on

IF OBJECT_ID(N'TEMPDB..#os_scheduler_fields') IS NOT NULL DROP TABLE #os_scheduler_fields

create table #os_scheduler_fields
(field_name nvarchar(128)
, description nvarchar(4000))

insert into #os_scheduler_fields select 'is_idle', '1 = Scheduler is idle. No workers are currently running.'
insert into #os_scheduler_fields select  'preemptive_switches_count', 'times that workers have switched to the preemptive mode to execute code that is outside SQL Server (for example, extended stored procedures and distributed queries).' 
insert into #os_scheduler_fields select  'context_switches_count', 'times a current running worker relinquished control of the scheduler to allow  other workers to run.  If a worker yields the scheduler and puts itself into the runnable queue and then finds no other workers, the worker will select itself. In this case, the context_switches_count is not updated, but the yield_count is updated.' 
insert into #os_scheduler_fields select 'idle_switches_count', 'Number of times the scheduler has been waiting for an event while idle. This column is similar to context_switches_count.'
insert into #os_scheduler_fields select 'current_tasks_count', 'Number of current tasks that are associated with this scheduler. This count includes Tasks that are waiting for a worker to execute them and Tasks that are currently waiting or running (in SUSPENDED or RUNNABLE state).'
insert into #os_scheduler_fields select 'runnable_tasks_count', 'Number of workers, with tasks assigned to them, that are waiting to be scheduled on the runnable queue.'
insert into #os_scheduler_fields select 'current_workers_count', 'Number of workers that are associated with this scheduler. This count includes workers that are not assigned any task.'
insert into #os_scheduler_fields select 'active_workers_count', 'Number of workers that are active. An active worker is never preemptive, must have an associated task, and is either running, runnable, or suspended.'
insert into #os_scheduler_fields select 'work_queue_count', 'Number of tasks in the pending queue. These tasks are waiting for a worker to pick them up.'
insert into #os_scheduler_fields select 'pending_disk_io_count', 'Number of pending I/Os that are waiting to be completed. Each scheduler has a list of pending I/Os that are checked to determine whether they have been completed every time there is a context switch. The count is incremented when the request is inserted. This count is decremented when the request is completed. This number does not indicate the state of the I/Os.'
insert into #os_scheduler_fields select 'load_factor', 'Internal value that indicates the perceived load on this scheduler. This value is used to determine whether a new task should be put on this scheduler or another scheduler.  
 SQL Server also uses a load factor of nodes and schedulers to help determine the best location to acquire resources. When a task is enqueued, the load factor is increased. '
insert into #os_scheduler_fields select 'yield_count', 'Internal value that is used to indicate progress on this scheduler. This value is used by the Scheduler Monitor to determine whether a worker on the scheduler is not yielding to other workers on time. This value does not indicate that the worker or task transitioned to a new worker.'
insert into #os_scheduler_fields select 'last_timer_activity', 'In CPU ticks, the last time that the scheduler timer queue was checked by the scheduler.'
insert into #os_scheduler_fields select 'failed_to_create_worker', 'Set to 1 if a new worker could not be created on this scheduler. This generally occurs because of memory constraints.'
 
select * from #os_scheduler_fields
 
