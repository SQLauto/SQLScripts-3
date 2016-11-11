

/* collect sql-based perfmon counters in a loop*/



set nocount on

/* some counters require comparing two points in time.  so, always need at least 2 loops*/

declare @loop_count            int = 5  -- loops not likely needed if persisting tables and running via sql job.
declare @loop_interval_seconds int = 1

declare @only_show_changes     bit = 0 -- there are a lot of counters.  set to 1 to exclude values where min(calc_value) = max(calc_value).  
                                       -- includes the @only_show_nonzero logic.
declare @only_show_nonzero     bit = 0 -- subset of logic above.  sometimes you want to see non-changing, non-zero values.

declare @pivoted_output_yn     bit = 0 -- pivoting creates columns for each loop's values.  set to 0 for better excel pivot tables, 1 for simpler ssms review.


/* select counters to include*/
declare @show_common_counters  bit = 1
declare @show_relationships    bit = 1  -- common ratios among sql counters.  for example, compilations per batch request; recompilations per compilation
                                        -- requires @show_common_counters = 1
declare @show_memory_counters  bit = 0
declare @show_cpu_counters     bit = 0
declare @show_tempdb_counters  bit = 0
declare @show_deprecated       bit = 0
declare @show_in_memory_oltp   bit = 0
declare @show_resource_govr    bit = 0
declare @show_buffer_pool_xtn  bit = 0
declare @show_query_store      bit = 0


/*read me - 
option to filter for specific object names.

execute the following query to get a list of object names. 
use the output to filter the data produced, by commenting out the ones you don't want, and then
uncommenting the join in the load of the #staging table.

select  object_name, count(*), 'insert into #include_objects select ''' + object_name + ''''
from sys.dm_os_performance_counters group by object_name order by count(*) desc
*/
    
-----------------------------------------------------------------------------------
if @loop_count < 2
   begin
      RAISERROR('Per-second counters require comparing two points in time, so @loop_count must be > 1.', 16, 1)
      return
   end
declare @now               datetime = getdate()   --make sure all records in the batch are inserted with same value
declare @server_instance    sysname = cast(serverproperty('servername') as sysname)
declare @server_start_time datetime = (select sqlserver_start_time from sys.dm_os_sys_info)
declare @duration          decimal(38, 3) = datediff(second, @server_start_time, @now)
declare @prior_datetime    datetime = null



declare @batch_counter       int = 1
declare @original_loop_value int = @loop_count

if object_id(N'tempdb..#staging')   is not null drop table #staging
if object_id(N'tempdb..#base')      is not null drop table #base
if object_id(N'tempdb..#numerator') is not null drop table #numerator
           
if object_id(N'tempdb..#current')   is not null drop table #current
if object_id(N'tempdb..#prior')     is not null drop table #prior
if object_id(N'tempdb..#history')   is not null drop table #history


create table #current
(     batch_datetime datetime
    , duration_sec   int
    , cntr_type      int
    , category       nvarchar(128)
    , object_name    nvarchar(128)
    , counter_name   nvarchar(128)
    , instance_name  nvarchar(128)
    , value          decimal(38,4)
    , join_value     varbinary(20)
)
  
create table #prior
(     prior_id       int identity(1, 1)
    , batch_id       int
    , batch_datetime datetime
    , duration_sec   decimal(34,3)
    , cntr_type      int
    , category       nvarchar(128)
    , object_name    nvarchar(128)
    , counter_name   nvarchar(128)
    , instance_name  nvarchar(128)
    , value          decimal(38,4)
    , join_value     varbinary(20)
)

create table #history
(     history_id     int identity(1, 1)
    , batch_id       int
    , batch_datetime datetime
    , duration_sec   decimal(34,3)
    , cntr_type      int
    , category       nvarchar(128)
    , object_name    nvarchar(128)
    , counter_name   nvarchar(128)
    , instance_name  nvarchar(128)
    , value          decimal(38,4)
    , calc_value     decimal(38,4)
    , join_value     varbinary(20)
)

/* load counters commonly used for a given resource/category.*/

/* select object_name
      , counter_name
      , count(*)
   from sys.dm_os_performance_counters
   group by object_name, counter_name
   order by counter_name
*/
if object_id(N'tempdb..#perfmon_category') is not null drop table #perfmon_category
 
create table #perfmon_category
(
      category            nvarchar(128)
    , counter_name        nvarchar(128)
    , evaluation_criteria nvarchar(1000)
    , notes               nvarchar(1000)
)

if @show_common_counters = 1
   begin
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'user connections'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'logins/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'logouts/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'connection resets/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'processes blocked'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'number of deadlocks/sec'

      insert into #perfmon_category (category, counter_name) select N'common counters',  N'logical connections'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'active temp tables'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'temp tables creation rate'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'transactions/sec'                                                                             
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'full scans/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'range scans/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'probe scans/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'table lock escalations/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'index searches/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'page splits/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'forwarded records/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'freespace scans/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'scan point revalidations/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'workfiles created/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'worktables created/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'Worktables From Cache Base'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'Worktables From Cache Ratio'
                                                             
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'page writes/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'buffer cache hit ratio'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'checkpoint pages/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'lazy writes/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'page life expectancy'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'free pages'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'free list stalls/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'readahead pages/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'local node page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'remote node page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'stolen pages'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'target pages'
                                                                  
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'memory grants pending'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'memory grants outstanding'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'target server memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'total server memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'free memory (kb)'          
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'database cache memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'sql cache memory (kb)'     
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'stolen server memory (kb)' 
                                                                                       
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'batch requests/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'sql compilations/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'sql re-compilations/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'auto-param attempts/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'failed auto-params/sec'
                                                               
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'lock waits/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'lock wait time (ms)'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'lock requests/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'lock timeouts/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'number of deadlocks/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'latch waits/sec'
      insert into #perfmon_category (category, counter_name) select N'common counters',  N'total latch wait time (ms)'

   end

if @show_memory_counters = 1
   begin
      insert into #perfmon_category (category, counter_name) select N'memory', N'target server memory (kb)'    
      insert into #perfmon_category (category, counter_name) select N'memory', N'total server memory (kb)'     
      insert into #perfmon_category (category, counter_name) select N'memory', N'stolen server memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'memory', N'free memory (kb)'             
      insert into #perfmon_category (category, counter_name) select N'memory', N'connection memory (kb)'       
      insert into #perfmon_category (category, counter_name) select N'memory', N'database cache memory (kb)'   
      insert into #perfmon_category (category, counter_name) select N'memory', N'lock memory (kb)'             
      insert into #perfmon_category (category, counter_name) select N'memory', N'log pool memory (kb)'         
      insert into #perfmon_category (category, counter_name) select N'memory', N'optimizer memory (kb)'        
      insert into #perfmon_category (category, counter_name) select N'memory', N'sql cache memory (kb)'        
 
      insert into #perfmon_category (category, counter_name) select N'memory', N'maximum workspace memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'memory', N'granted workspace memory (kb)'
      insert into #perfmon_category (category, counter_name) select N'memory', N'reserved server memory (kb)'  
      insert into #perfmon_category (category, counter_name) select N'memory', N'memory grants outstanding'    
      insert into #perfmon_category (category, counter_name) select N'memory', N'memory grants pending'        
      insert into #perfmon_category (category, counter_name) select N'memory', N'lock blocks'                  
      insert into #perfmon_category (category, counter_name) select N'memory', N'lock blocks allocated'        
      insert into #perfmon_category (category, counter_name) select N'memory', N'lock owner blocks'            
      insert into #perfmon_category (category, counter_name) select N'memory', N'lock owner blocks allocated'  
                                                               
      insert into #perfmon_category (category, counter_name) select N'memory', N'checkpoint pages/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'free list stalls/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'free pages'
      insert into #perfmon_category (category, counter_name) select N'memory', N'lazy writes/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'page reads/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'page writes/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'readahead pages/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'local node page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'remote node page lookups/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'page life expectancy'
      insert into #perfmon_category (category, counter_name) select N'memory', N'stolen pages'
      insert into #perfmon_category (category, counter_name) select N'memory', N'target pages'
      insert into #perfmon_category (category, counter_name) select N'memory', N'memory grant timeouts/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'memory grants/sec'
      insert into #perfmon_category (category, counter_name) select N'memory', N'reduced memory grants/sec'
end

if @show_cpu_counters = 1
   begin
      insert into #perfmon_category (category, counter_name) select N'cpu', N'batch requests/sec'
      insert into #perfmon_category (category, counter_name) select N'cpu', N'sql compilations/sec'
      insert into #perfmon_category (category, counter_name) select N'cpu', N'sql re-compilations/sec'   
                                                                           
      insert into #perfmon_category (category, counter_name) select N'cpu', N'query optimizations/sec' 
      insert into #perfmon_category (category, counter_name) select N'cpu', N'cpu usage %'
   end

if @show_tempdb_counters = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'TempDB', counter_name
         from sys.dm_os_performance_counters
         where counter_name like N'%temp tables%'
         or  instance_name = N'tempdb'
         or (counter_name like N'%version%'and object_name like N'%transactions%')
         or counter_name in (N'workfiles created/sec', N'worktables created/sec' ) 
   end

if @show_deprecated = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'Deprecated', counter_name
         from sys.dm_os_performance_counters
         where object_name like N'%dep%'
   end

if @show_in_memory_oltp = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'In-Memory OLTP', counter_name
         from sys.dm_os_performance_counters
         where object_name like N'%xtp%'
   end

if @show_resource_govr = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'Resource Governor', counter_name
         from sys.dm_os_performance_counters
         where object_name like N'%workload group stats%'
            or object_name like N'%resource pool stats%'
   end

if @show_buffer_pool_xtn = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'Buffer Pool Extension', counter_name
         from sys.dm_os_performance_counters
         where object_name like N'%buffer manager%'
            and counter_name like 'Extension%'
   end

if @show_query_store = 1
   begin
      insert into #perfmon_category (category, counter_name) 
         select N'Query Store', counter_name
         from sys.dm_os_performance_counters
         where object_name like N'%Query Store%'
   end


/* select distinct  object_name, counter_name from #history   

   select object_name, counter_name, count(*)
   from sys.dm_os_performance_counters
   where object_name like '%dep%'
   group by object_name, counter_name
   order by object_name, counter_name
*/

/* this is delicate, and likely to break for new versions of sql.
two likely problems:
1.  new counter types
2.  the fractional items that join #base and #numerator assume the value and base are listed in a certain order.  
    at least 1 set (filetable) isn't.

to address, i return errors if unhandled counter types are found, and do a high-level check for join problems.*/

declare @unhandled_counter_types nvarchar(1000) = N''
select distinct @unhandled_counter_types = cast(cntr_type as nvarchar(10)) + N', '
from sys.dm_os_performance_counters
where cntr_type not in ( 1073939712
                       , 1073874176
                       , 537003264
                       , 65792
                       , 272696576
                       , 272696320)

if @unhandled_counter_types <> N''
   begin
      select 'This sql version has the following counter types not handled in this logic: @unhandled_counter_types.  The first output is a list of those counters.'

      select distinct object_name, counter_name, cntr_type as unhandled_counter_type
      from sys.dm_os_performance_counters
      where cntr_type not in ( 1073939712
                             , 1073874176
                             , 537003264
                             , 65792
                             , 272696576
                             , 272696320)
   end  --if @unhandled_counter_types <> ''

declare @counterprefix nvarchar(50)
set @counterprefix = case when @@servicename = 'mssqlserver'
                           then N'sqlserver:'
                           else N'mssql$' + @@servicename + N':'
                           end ;

while @loop_count >= 1
   begin

      set @now = getdate()

      if object_id(N'tempdb..#staging')   is not null drop table #staging
      if object_id(N'tempdb..#base')      is not null drop table #base
      if object_id(N'tempdb..#numerator') is not null drop table #numerator

      truncate table #current

      create table #staging
      (
            row_num       int identity(1, 1)
          , object_name   nchar(128)
          , counter_name  nvarchar(4000)
          , instance_name nchar(128)
          , cntr_value    bigint
          , cntr_type     int
          , category      nvarchar(128)
          , join_value    varbinary(8000)
      )
  

         if exists (select 1 from #perfmon_category)
            begin
               insert into #staging
                  select replace(c.object_name, @counterprefix, '') as object_name
                   , replace(c.counter_name, N'avg ', N'') as counter_name -- problem with filetable
                   , instance_name 
                   , cntr_value    
                   , cntr_type     
                   , category
                   , hashbytes('sha1', c.object_name + N'|' + c.counter_name + N'|' + coalesce(instance_name, N'x')) as join_value
                  from sys.dm_os_performance_counters c
                  --join #include_objects i on i.object_name = c.object_name
                  join #perfmon_category cat on cat.counter_name = c.counter_name

               /* without this, i pull in all the counters that have 'tempdb' as an instance name.
                  long-term fix is to add instance to category.*/
               delete from #staging
               where category = N'tempdb' 
                  and not (counter_name like N'%temp tables%'
                           or  instance_name = N'tempdb'
                           or (counter_name like N'%version%'and object_name like N'%transactions%')
                           or counter_name in (N'workfiles created/sec', N'worktables created/sec')) 
            end
         else
            begin
               insert into #staging
                  select c.object_name   
                   , replace(c.counter_name, N'avg ', N'') as counter_name -- problem with filetable
                   , instance_name 
                   , cntr_value    
                   , cntr_type     
                   , '' as category
                   , hashbytes('sha1', c.object_name + N'|' + c.counter_name + N'|' + coalesce(instance_name, N'x')) as join_value
                  from sys.dm_os_performance_counters c

            end




         --select cntr_type, count(*) from #staging group by cntr_type

         select row_num - 1 as row_num2
         , *
         into #base
         from #staging
         where cntr_type = 1073939712

         select * 
         into #numerator
         from #staging
         where cntr_type in (1073874176, 537003264)

      /* for point in time metrics, values can go directly to #history*/
      insert into #history
         select 
           @batch_counter
         , @now       as batch_datetime
         , @duration  as duration_sec 
         , n.cntr_type
         , n.category
         , ltrim(rtrim(n.object_name))
         , ltrim(rtrim(n.counter_name))
         , ltrim(rtrim(case when n.instance_name = N'' then N'.' else n.instance_name end )) as instance_name
         , case when b.cntr_value = 0 
                  then 0 
                  else cast(n.cntr_value*1.0/b.cntr_value as decimal(34, 4)) * case when n.cntr_type = 1073874176 
                                                                                    then 1 else 100 end  end as value
         , case when b.cntr_value = 0 
                  then 0 
                  else cast(n.cntr_value*1.0/b.cntr_value as decimal(34, 4)) * case when n.cntr_type = 1073874176 
                                                                                    then 1 else 100 end  end as value
         , b.join_value
         from #base b
         join #numerator n on n.row_num = b.row_num2
         union all
         select 
           @batch_counter
         , @now       as batch_datetime
         , @duration  as duration_sec 

         , x.cntr_type
         , x.category
         , ltrim(rtrim(x.object_name))
         , ltrim(rtrim(x.counter_name))
         , ltrim(rtrim(case when x.instance_name = N'' then N'.' else x.instance_name end )) as instance_name
         , x.cntr_value
         , x.cntr_value
         , x.join_value
         from #staging x
         where cntr_type = 65792

      insert into #current
         select 
           @now       as batch_datetime
         , @duration  as duration_sec 
         , x.cntr_type
         , x.category
         , ltrim(rtrim(x.object_name))
         , ltrim(rtrim(x.counter_name))
         , ltrim(rtrim(case when x.instance_name = N'' then N'.' else x.instance_name end )) as instance_name
         , cntr_value
         , x.join_value
         from #staging x
         where cntr_type in ( 272696576, 272696320)

      if @batch_counter = 1
         begin
            insert into #prior
               select @batch_counter, * from #current
      
            insert into #history
               select @batch_counter 
                    , @now       as batch_datetime
                    , @duration  as duration_sec 
                    , c.cntr_type
                    , c.category
                    , c.object_name
                    , c.counter_name
                    , c.instance_name
                    , c.value
                    , c.value / @duration
                    , c.join_value
               from #current c


            set @prior_datetime = @now
            set @now = getdate()
         end --if @batch_counter = 1
      else 
         begin 
            set @duration = datediff(ms, @prior_datetime, @now)/1000.0

            insert into #history
               select @batch_counter
               , c.batch_datetime
               , @duration  
               , c.cntr_type
               , c.category
               , c.object_name
               , c.counter_name
               , c.instance_name
               , c.value - p.value
               , (c.value - p.value) / @duration
               , c.join_value
               from #current    c
               full join #prior p on p.join_value = c.join_value

            truncate table #prior

            insert into #prior
               select @batch_counter, * from #current

            set @now = getdate()

         end  --if @batch_counter <> 1

      set @loop_count = @loop_count - 1
      set @batch_counter = @batch_counter + 1
      set  @prior_datetime = @now 

      waitfor delay @loop_interval_seconds




   end -- while @loop_count >= 1


if @pivoted_output_yn = 0
   begin
      if @only_show_changes = 1  
         begin
            select
                  batch_id
                , batch_datetime
                --, cntr_type
                , @counterprefix as instance
                , category
                , replace(object_name, @counterprefix, N'') as object_name
                , counter_name
                , instance_name
                , value
                , duration_sec
                , calc_value
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name as object_counter
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                --, h.join_value
            from #history h
            left join (
                        select join_value
                        from #history 
                        where batch_id > 1
                        group by join_value 
                        having min(calc_value) = max(calc_value)
            ) x on x.join_value = h.join_value
            where x.join_value is null
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end -- if @only_show_changes = 1  
      else if @only_show_nonzero = 1 
         begin
            select
                  batch_id
                , batch_datetime
                --, cntr_type
                , category
                , @counterprefix as instance
                , replace(object_name, @counterprefix, N'') as object_name
                , counter_name
                , instance_name
                , value
                , duration_sec
                , calc_value
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name as object_counter
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                --, h.join_value
            from #history h
            left join (
                        select join_value
                        from #history 
                        where batch_id > 1
                        group by join_value 
                        having min(calc_value) = 0
                           and max(calc_value) = 0
            ) x on x.join_value = h.join_value
            where x.join_value is null
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end --else if @only_show_nonzero = 1
      else
         begin
            select
                  batch_id
                , batch_datetime
                --, cntr_type
                , category
                , @counterprefix as instance
                , replace(object_name, @counterprefix, N'') as object_name
                , counter_name
                , instance_name
                , value
                , duration_sec
                , calc_value
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name as object_counter
                , replace(object_name, @counterprefix, N'') + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                --, h.join_value
            from #history h
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end
   end --if @pivoted_output_yn = 0
else --if @pivoted_output_yn = 1
   begin

      if object_id(N'tempdb..#history_to_pivot') is not null drop table #history_to_pivot
 
      create table #history_to_pivot
      (
            batch_id                int
          --, batch_datetime          datetime
          --, instance                nvarchar(50)
          , category                nvarchar(128)
          , object_name             nvarchar(128)
          , counter_name            nvarchar(128)
          , instance_name           nvarchar(128)
          --, value                   decimal(38,4)
          --, duration_sec            decimal(34,3)
          , calc_value              decimal(38,4)
          --, object_counter          nvarchar(261)
          , object_counter_instance nvarchar(394)
          , join_value varbinary(20)
      )

      if @only_show_changes = 1  
         begin
            insert into #history_to_pivot
            select
                  batch_id
                , category
                , object_name
                , counter_name
                , instance_name 
                , calc_value
                , object_name + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                , h.join_value
            from #history h
            left join (
                        select join_value
                        from #history 
                        where batch_id > 1
                        group by join_value 
                        having min(calc_value) = max(calc_value)
            ) x on x.join_value = h.join_value
            where x.join_value is null
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end -- if @only_show_changes = 1  
      else if @only_show_nonzero = 1 
         begin
            insert into #history_to_pivot
            select
                  batch_id
                , category
                , object_name
                , counter_name
                , instance_name 
                , calc_value
                , object_name + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                , h.join_value
            from #history h
            left join (
                        select join_value
                        from #history 
                        where batch_id > 1
                        group by join_value 
                        having min(calc_value) = 0
                           and max(calc_value) = 0
            ) x on x.join_value = h.join_value
            where x.join_value is null
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end --else if @only_show_nonzero = 1
      else
         begin
            insert into #history_to_pivot
            select
                  batch_id
                , category
                , object_name
                , counter_name
                , instance_name 
                , calc_value
                , object_name + N' | ' + counter_name  + N' | ' + instance_name as object_counter_instance
                , h.join_value
            from #history h
            order by category
                     , object_name
                     , counter_name
                     , instance_name
                     , batch_id
         end
----------------------------------------------------------------------------------------

      /* start pivot*/
      if not exists (select 1 from #history_to_pivot)
         begin
            select 'No changes in any selected counters.  Cannot pivot.  Exiting.'
            return
         end 
       else 
         begin --pivot logic
            if object_id(N'tempdb..#cols') is not null drop table #cols
            if object_id(N'tempdb..##pivoted_output') is not null drop table ##pivoted_output

            select distinct batch_id, join_value   
            into #cols
            from #history_to_pivot
            order by join_value, batch_id

            /* build csv list. */
            declare  @cols as nvarchar(max)
                   , @sql as nvarchar(max)

            set @cols = stuff(
                              (select N',' + quotename(batch_id) as [text()]
                              from (select distinct batch_id from #history_to_pivot) as y
                              order by batch_id
                              for xml path('')), 1, 1, '')

            /* build pivot*/

            set @sql = N'select * into ##pivoted_output from 
                           (select 
                                h.category
                              , h.object_name
                              , h.counter_name
                              , h.instance_name
                              --, h.object_counter_instance
                              , h.batch_id
                              , h.calc_value
                            from #history_to_pivot h
                            join #cols   c on c.batch_id = h.batch_id and c.join_value = h.join_value) as d
                        pivot (max(calc_value) for batch_id in (' + @cols + ')) as p   ;'

               print @sql
               exec sp_executesql @sql

               select * from ##pivoted_output
               order by category, object_name, counter_name, instance_name
            end --pivot logic
      end --if @pivoted_output_yn = 1




/* optional output, getting ratios between values.
these were taken from pal's sql template, maintained by david pless*/
if @show_common_counters = 1 and @show_relationships  = 1
   begin

      create index ix_match on #history(batch_id, counter_name) include (calc_value, value)

      select a.batch_id
         , a.batch_datetime
         , a.calc_value as batch_requests_per_sec--, a.cntr_value
         , b.calc_value as compilations_per_sec--, b.cntr_value
         , c.calc_value as recompilations_per_sec--, c.cntr_value
         , d.calc_value as forwarded_records
         , e.calc_value as free_space_scans
         , f.calc_value as full_scans
         , g.calc_value as index_searches
         , h.calc_value as page_splits
         , i.calc_value as page_lookups
         , j.calc_value as lock_timeouts
         , k.calc_value as total_lock_requests
         , l.calc_value as latch_waits_per_sec
         , m.calc_value as total_latch_wait_time_ms
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(b.calc_value/a.calc_value as decimal(10, 2)) end as compilations_per_batch_req
         , case when coalesce(b.calc_value, 0) = 0 then 0 else cast(c.calc_value/b.calc_value as decimal(10, 2)) end as recompilations_per_compilation
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(d.calc_value/a.calc_value as decimal(10, 2)) end as forwarded_records_per_batch_req
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(e.calc_value/a.calc_value as decimal(10, 2)) end as free_space_scans_per_batch_req
         , case when coalesce(g.calc_value, 0) = 0 then 0 else cast(f.calc_value/g.calc_value as decimal(10, 2)) end as full_scans_per_index_search
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(h.calc_value/a.calc_value as decimal(10, 2)) end as page_splits_per_batch_req
         , case when coalesce(i.calc_value, 0) = 0 then 0 else cast(a.calc_value/i.calc_value as decimal(10, 2)) end as batch_requests_per_page_lookup
         , case when coalesce(m.calc_value, 0) = 0 then 0 else cast(l.calc_value/m.calc_value as decimal(10, 2)) end as latch_waits_per_sec_per_total_latch_wait_time_ms
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(j.calc_value/a.calc_value as decimal(10, 2)) end as lock_timeouts_per_batch_req
         , case when coalesce(a.calc_value, 0) = 0 then 0 else cast(k.calc_value/a.calc_value as decimal(10, 2)) end as lock_requests_per_batch_req
         --if , case when cast(b.calc_value*100.0/a.calc_value as decimal(10, 2))  < 10 then 0 else 1 end as is_problem_compilations_percent_of_batches_calc
         --, case when cast(c.calc_value*100.0/b.calc_value as decimal(10, 2))  < 10 then 0 else 1 end as is_problem_recompilations_percent_of_compilations_calc
         --, 10 as goal_compilations_percent_of_batches_calc
         --, 10 as goal_recompilations_percent_of_compilations_calc
      from #history a
      left join (select * from #history where counter_name = N'sql compilations/sec')    b on b.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'sql re-compilations/sec') c on c.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'forwarded records/sec')   d on d.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'freespace scans/sec')     e on e.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'full scans/sec')          f on f.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'index searches/sec')      g on g.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'page splits/sec')         h on h.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'page lookups/sec')        i on i.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'lock timeouts')           j on j.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'total lock requests')     k on k.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'latch waits/sec')         l on l.batch_id = a.batch_id 
      left join (select * from #history where counter_name = N'total latch wait time ms')m on m.batch_id = a.batch_id 
      where a.counter_name = N'batch requests/sec'
end -- if @show_common_counters = 1 and @show_relationships  = 1



