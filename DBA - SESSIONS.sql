/* review session info:

overview
      this query is meant to improve on traditional sp_who* logic in several ways:
         1 - it provides a great deal of filtering flexibility.
               users can choose single spids or all sessions, but can also filter by status, or duration, or tasks currently waiting, etc.
         2 - it includes data from a wide array of dmvs, providing a much richer data set
               information is pulled from sys.dm_exec_sessions, sys.dm_exec_requests, sys.dm_exec_query_memory_grants, and others.
         3 - users can select optional, more costly outputs such as query plans, dbcc inputbuffer, or session lock info
         4 - users can persist the data for later review, to generate alerts, etc.  options for duration, frequency, and retention are provided.
         5 - users can poll the data at constant intervals, by date range; by timer.  of course, it could be added to a job instead, but for a 5 minute collection 
                having the option in the query is convenient.

notes
      1.  i did not wrap this in a stored proc, primarily because the large number of parameters can be awkward.  also, i use this primarily for ad hoc collection.
          feel free to use a proc if desired.
      2.  i wrote this on a case-insensitive collation.  i figure you can go there if you need to on your own.
      3.  this is not the lightest of queries, on a busy system with hundreds of sessions.  don't try collecting every second for all sessions with query plans included, 
          or some other foolishness.  at least run it ad hoc for brief periods to get a sense of its impact (which is conveniently included in the output itself).
      4.  speaking of performance - i use some techniques in here that i abhor.  normally i would  never have such a complex where clause as i do below.  but, with session
            information being so transitory, it doesn't make a lot of sense to collect info piecemeal in temp tables. 
      5.  be aware that blocking and/or open transactions may be sleeping.  don't filter for status if you are looking for specific problems.

author - john kauffman
*/
set nocount on
------------------------------------------------------------
/* user input - select filter logic*/
-----------------------------------------------------------
declare @session_id        int = null  /* if @session_id is not null, that setting overrides all other filters.  a single session_id is returned*/

declare @show_all_sessions bit = 0     /* if @show_all_sessions = 1, that setting overrides all filters except @session_id*/

/* all other filters are cumulative.*/
declare @show_system_or_user_sessions                   nchar(1)       = N'u'  -- options include 's', 'u', '*'
declare @statuses_to_exclude                            nvarchar(1000) = N'' -- n', sleeping, dormant'  -- takes csv list of statuses (or '' for none).  options include running, sleeping, dormant, or preconnect
declare @only_show_blocking                             bit = 0   -- includes blocked and blocker sessions.  doesn't work well with others.
declare @only_show_waiting_sessions                     bit = 0
declare @only_show_uncommitted_transactions             bit = 0
declare @only_show_long_running_requests_or_sessions    bit = 0  
declare @long_running_if_more_seconds_than              int = 5

declare @collapse_wait_threads bit = 1 -- typically kept at 1.  cxpackets  can cause separate outputs for each thread.  

------------------------------------------------------------
/* user input - select optional, possibly costly outputs*/
/* not coded yet*/
------------------------------------------------------------
declare @include_query_plan   bit = 1
declare @include_input_buffer bit = 0
declare @include_lock_info_for_current_db bit = 1  -- separate output b/c of 1 to many relationship.  
                                                    -- lock info is a db-level view.  don't want to loop over dbs- too costly.  
                                                    -- only sessions that met filter criteria are included
declare @lock_info_summary_or_detail  char(1) = 's' -- options are 's', 'd', or '*' for both
declare @restrict_lock_info_to_request_id bit = 0   -- get full lock history for session, or only current request

------------------------------------------------------------
/* user input - polling options*/  /* for long runs or scheduled times, use a sql job.*/
/* not coded yet*/
------------------------------------------------------------
declare @looping_interval_ms      int = 1000
declare @total_duration_seconds   int = 300

------------------------------------------------------------
/* user input - persisting the data*/
------------------------------------------------------------
declare @save_output_yn                bit = 0
declare @save_to_temp_table            bit = 1 -- temp table created is named #session_temp
declare @persist_to_table   nvarchar(1000) = null  -- may include 4 part naming for linked servers.  
declare @create_table_if_needed        bit = 1 -- may not work, depending on permissions

--------------------------------------------------------------------------
/* set variables based on precedence*/
--------------------------------------------------------------------------
if @session_id is not null set @show_all_sessions = 0

if @session_id is not null or @show_all_sessions = 1
begin
   set @only_show_blocking                 = 0
   set @only_show_long_running_requests_or_sessions    = 0
   set @only_show_uncommitted_transactions = 0
   set @only_show_waiting_sessions         = 0
end
------------------------------------------------------------------------------------
/* build table for csv list of tables.
      not how i'd normally do this, but i don't want a dependency on an external function*/
if object_id(N'tempdb..#statuses') is not null drop table #statuses

create table #statuses (status nvarchar(30))

   insert into #statuses select 'running'
   insert into #statuses select 'sleeping' 
   insert into #statuses select 'dormant' 
   insert into #statuses select 'preconnect'

if @statuses_to_exclude <>  '' delete from #statuses where charindex(status, @statuses_to_exclude) > 0

---------------------------------------------------------------------------------------
/* pulling input buffer info*/
if object_id(N'tempdb..#spid_cmds') is not null drop table #spid_cmds
create table #spid_cmds
(sqlid int identity, spid int, eventtype varchar(100), parameters int, command varchar(5000))

/* because dbcc inputbuffer only takes a single spid, i'll have to loop to get results.
   for efficiency, i only want to loop over the ones that meet the filter criteria selected.
   but, the input buffer could be cleared by the time that happens.  so, for a single spid, i 
   get the info up front.  looping logic is done right before the result set is returned*/

if @include_input_buffer = 1 and @session_id is not null
   begin
	   insert into #spid_cmds (eventtype, parameters, command)
   		   exec('dbcc inputbuffer( ' + @session_id + ')')
   end


/* adding sort by logic made me need dynamic sql.  i took it out.  may add it back once the filters are known to work better*/

--declare @sort_by varchar(300)  = 'host_name desc'--program_name, login_name, db_name, status desc, session_id desc' --any column in output, or csv list.  add 'asc' or 'desc' as desired.  
--                                   --quick list for copying: 'session_id', 'blocking_session_id', 'db_name', session_total_elapsed_time', 'login_name', 'program_name', 'host_name', 'status', 'req_wait_type'
--                                   -- use 'blocking chain' to get lead-blocker, child-blockers, and blocked sessions

if object_id(N'tempdb..#x')      <> 0 drop table #x

create table #x
      (session_id                   smallint      
      , uncommitted_tran            bit
      , blocking                    bit
      , blocking_session_id         smallint
      , long_running                bit
      , waiting                     bit
      , db_name                     nvarchar(128)
      , status                      nvarchar(30)
      , login_time                  datetime
      , resource_pool               sysname
      , work_group                  sysname
      , req_wait_type               nvarchar(60)
      , wait_description            nvarchar(1024)
      , input_buffer_cmd            varchar(5000)
      , req_command                 nvarchar(1600)
      , sql_statement               nvarchar(max)
      , object_id                   int
      , object_name                 nvarchar(128)
      , percent_complete            real
      , estimated_completion_time   bigint
      , host_name                   nvarchar(128)
      , program_name                nvarchar(128)
      , client_interface_name       nvarchar(32)
      , login_name                  nvarchar(128)
      , original_login_name         nvarchar(128)
      , session_total_elapsed_time  int
      , last_request_start_time     datetime
      , last_request_end_time       datetime
      , req_start_time              datetime
      , req_total_elapsed_time      int
      , req_wait_time               int
      , req_last_wait_type          nvarchar(60)
      , req_row_count               bigint
      , transaction_isolation_level smallint
      , concat_null_yields_null     bit
      , arithabort                  bit
      , ansi_padding                bit
      , ansi_nulls                  bit
      , deadlock_priority           int
      , nest_level                  int
      , query_plan                  xml
      , session_cpu_time_ms         int
      , session_reads               bigint
      , session_logical_reads       bigint
      , session_writes              bigint
      , request_cpu_time_ms         int
      , request_reads               bigint
      , request_logical_reads       bigint
      , request_writes              bigint
      , scheduler_id                int
      , dop                         smallint
      , memory_grant_time           datetime
      , requested_memory_kb         bigint
      , granted_memory_kb           bigint
      , required_memory_kb          bigint
      , used_memory_kb              bigint
      , max_used_memory_kb          bigint
      , query_cost                  float
      , timeout_sec                 int
      , wait_order                  int
      , is_next_candidate           bit
      , wait_time_ms                bigint
      , group_id                    int
      , is_small                    tinyint
      , ideal_memory_kb             bigint
      , row_num                     int

      )
/*pull info.  filter out what you can in this round, without making the where clause too complex*/
insert into #x
select s.session_id
   , 0 
   , 0
   , coalesce(r.blocking_session_id, 0)
   , 0
   , 0
   , d.name                           as db_name
   , s.status
   , s.login_time
   , rp.name                          as resource_pool
   , wg.name                          as work_group
   , r.wait_type                      as req_wait_type
   , w.resource_description           as wait_description
   , null                             as input_buffer_cmd
   , r.command as req_command
   ,     (select top 1 substring(s2.text,statement_start_offset / 2+1 , 
         ( (case when statement_end_offset = -1 
            then (len(convert(nvarchar(max),s2.text)) * 2) 
            else statement_end_offset end)  - statement_start_offset) / 2+1))  as sql_statement
   , o.object_id
   , o.name                           as object_name
   , r.percent_complete
   , r.estimated_completion_time
   , s.host_name
   , s.program_name
   , s.client_interface_name
   , s.login_name
   , s.original_login_name
   , s.total_elapsed_time             as session_total_elapsed_time
   , s.last_request_start_time        
   , s.last_request_end_time          
   , r.start_time                     as req_start_time
   , r.total_elapsed_time             as req_total_elapsed_time
   , r.wait_time                      as req_wait_time
   , r.last_wait_type                 as req_last_wait_type
   , r.row_count                      as req_row_count
   , r.transaction_isolation_level
   , r.concat_null_yields_null
   , r.arithabort
   , r.ansi_padding
   , r.ansi_nulls
   , r.deadlock_priority
   , r.nest_level
   ,  eqp.query_plan
   , s.cpu_time
   , s.reads
   , s.logical_reads
   , s.writes
   , r.cpu_time
   , r.reads
   , r.logical_reads
   , r.writes
   , mg.scheduler_id
   , mg.dop 
   , mg.grant_time as memory_grant_time
   , mg.requested_memory_kb
   , mg.granted_memory_kb
   , mg.required_memory_kb
   , mg.used_memory_kb
   , mg.max_used_memory_kb
   , mg.query_cost
   , mg.timeout_sec
   , mg.wait_order
   , mg.is_next_candidate
   , mg.wait_time_ms
   , mg.group_id  
   , mg.is_small
   , ideal_memory_kb 
   , row_number() over(partition by s.session_id order by s.session_id)
from sys.dm_exec_sessions                         as  s
join sys.resource_governor_workload_groups        as wg on wg.group_id = s.group_id
join sys.resource_governor_resource_pools         as rp on rp.pool_id = wg.pool_id
left join sys.dm_exec_requests                    as  r on r.session_id = s.session_id
left join sys.databases                           as  d on d.database_id = r.database_id
left outer join sys.dm_os_waiting_tasks           as  w on w.session_id = r.session_id
outer apply sys.dm_exec_sql_text (sql_handle)     as s2
outer apply sys.dm_exec_query_plan(r.plan_handle) as eqp
left join sys.objects                             as  o on o.object_id = s2.objectid
left join sys.dm_exec_query_memory_grants         as mg on mg.request_id = r.request_id and mg.session_id = r.session_id
where (@session_id is not null and s.session_id = @session_id)
      or (@session_id is  null and
            (
              (@show_all_sessions = 0  
               and (  (@show_system_or_user_sessions in ('s') and s.session_id <= 50) 
                   or (@show_system_or_user_sessions in ('u') and s.session_id >  50)
                   or (@show_system_or_user_sessions in ('*')                       ) 
                   ) 
               and (s.status in (select status from #statuses))
              )
            )
      or @show_all_sessions = 1
         )
   

---------------------------------------------------------------------------------
/* categorize problem types*/
---------------------------------------------------------------------------------
update #x 
set blocking = 1 
where coalesce(blocking_session_id, 0) <> 0 

update #x 
set blocking = 1 
where session_id in (select blocking_session_id from #x where coalesce(blocking_session_id, 0) <> 0 )

update x 
set uncommitted_tran = 1
from #x x
where exists 
      (
      select * 
      from sys.dm_tran_session_transactions as t
      where t.session_id = x.session_id
      )
      and not exists 
      (
      select * 
      from sys.dm_exec_requests as r
      where r.session_id = x.session_id
      )
and datediff(second, last_request_end_time, getdate()) > @long_running_if_more_seconds_than
and not (client_interface_name = 'odbc' and login_name in ('tms', 'ukey'))

update x 
set long_running = 1
from #x x
where datediff(second, req_start_time, getdate()) > @long_running_if_more_seconds_than
and @only_show_long_running_requests_or_sessions = 1



update x 
set waiting = 1
from #x x
where req_wait_type is not null

if @only_show_blocking = 1
delete from #x where blocking = 0

if @only_show_waiting_sessions = 1
delete from #x where waiting = 0

if @only_show_uncommitted_transactions = 1
delete from #x where uncommitted_tran = 0

if @only_show_long_running_requests_or_sessions = 1
delete from #x where long_running = 0




        -- and (s.status in (select status from #statuses))
        -- and (@only_show_blocking = 1 and (r.blocking_session_id is not null 
        --                                  or s.session_id in (select blocking_session_id from sys.dm_exec_requests)
        --                                  )
        --      or @only_show_blocking = 0)
        --and ((@only_show_waiting_sessions = 1 and r.wait_type is not null)
        --      or @only_show_waiting_sessions = 0)
        --and ((@only_show_long_running_requests_or_sessions = 1 and datediff(second, r.start_time,getdate()) > @long_running_if_more_seconds_than)
        --     or @only_show_long_running_requests_or_sessions = 0)
        --and ((@only_show_long_running_sessions = 1 and datediff(second, s.login_time,getdate()) > @long_running_if_more_seconds_than)
        --     or @only_show_long_running_sessions = 0)


if @collapse_wait_threads = 1
   begin
      select  * 
      from #x
      where row_num = 1
      order by session_id
   end
else
   begin
      select  * 
      from #x
      order by session_id
   end



----drop table #x
----drop table #y

/* need to loop over input buffer*/

   if @include_lock_info_for_current_db = 1
   begin
   select request_session_id, resource_type, request_mode, request_status, count(*)
   from sys.dm_tran_locks l
   join #x x on x.session_id = l.request_session_id
   --where not (resource_type = 'database' and request_mode = 's') 
   group by request_session_id, resource_type, request_mode, request_status
   order by request_session_id, resource_type, request_mode, request_status
   end




/* dynamic sql needed for sort options.  code not used, but i may get back to it 
at some point.  filter logic is wrong.  that's the main reason i'm dumping it - it's 
too hard to troubleshoot the filters*/
/*
      declare @sql_text varchar(8000)
      declare @table_name sysname = '##y_'+ cast(@@spid as varchar(10)) 
      + '_' + cast(year(getdate()) as varchar(50)) 
      + '_' + cast(month(getdate()) as varchar(50)) 
      + '_' + cast(datepart(day, getdate()) as varchar(50))+ '_' + cast(datepart(hour, getdate())as varchar(50))
      + '_' + cast(datepart(minute, getdate()) as varchar(50))

      set @sql_text = '
      declare @show_system_sessions bit = <<@show_system_sessions>>
      declare @show_sleeping_user_sessions bit = <<@show_sleeping_user_sessions>>
      declare @show_uncommitted_transactions bit = <<@show_uncommitted_transactions>>
      declare @show_blocking bit  = <<@show_blocking>>
      declare @show_long_running_requests bit  = <<@show_long_running_requests>>
      declare @show_long_running_sessions bit= <<@show_long_running_sessions>>
      declare @show_waiting_sessions bit = <<@show_waiting_sessions>>

      select *
      into @table_name
      from #x
      where session_id > 50
      and status <> ''sleeping''
      union all
      select *
      from #x
      where (@show_system_sessions = 1 and session_id <=50)
      union all
      select *
      from #x
      where (@show_sleeping_user_sessions = 1 and status = ''sleeping'')
      order by login_name, session_id

      if @show_uncommitted_transactions = 1
      delete from @table_name where 
      select *
      from @table_name
      where @show_uncommitted_transactions  =  uncommitted_tran
      or ((@show_blocking = 1 and coalesce(blocking_session_id, 0) > 0) or (@show_blocking = 0 and coalesce(blocking_session_id, 0) = 0))
      or @show_long_running_requests = long_running
      or @show_long_running_sessions = long_running
      or @show_waiting_sessions = waiting
      order by ' + @sort_by
      + char(10) + char(13)
      + 'drop table ' + @table_name

      set @sql_text = replace(@sql_text, '@table_name', @table_name)
      set @sql_text = replace(@sql_text, '<<@show_system_sessions>>', @show_system_sessions)
      set @sql_text = replace(@sql_text, '<<@show_sleeping_user_sessions>>', @show_sleeping_user_sessions)
      set @sql_text = replace(@sql_text, '<<@show_uncommitted_transactions>>', @show_uncommitted_transactions)

      set @sql_text = replace(@sql_text, '<<@show_blocking>>', @show_blocking)
      set @sql_text = replace(@sql_text, '<<@show_long_running_requests>>', @show_long_running_requests)
      set @sql_text = replace(@sql_text, '<<@show_long_running_sessions>>', @show_long_running_sessions)
      set @sql_text = replace(@sql_text, '<<@show_waiting_sessions>>', @show_waiting_sessions)
*/