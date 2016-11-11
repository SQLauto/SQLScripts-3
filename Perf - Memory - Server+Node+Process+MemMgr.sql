/* 
Script:         Ad Hoc SQL memory analysis

Purpose:        returns 
                  server and process-level memory utilization similar to what is seen in windows resource monitor.
                  node-level info (e.g. local vs foreign memory)
                  memory manager breakdown (e.g. buffer pool, connection, lock, optimizer, plan cache).
                     memory broker, clerk, and object info not included.
                  additional information (e.g., page life expectancy, memory grants outstanding, etc.) are also included.

Functionality:  User can set the number of loops and loop interval for data collection.
                user can select units in kb, mb, or gb, where appropriate.
                Output includes min and max values for each metric, with percent difference.
                Output includes metadata about each field - source, calculation used, definition, equivalent values.
                The metrics' scope (e.g., server, process, memory manager, memory grant) is listed for each metric.

Source:         data pulled from sys.dm_os_sys_info, sys.dm_os_memory_info, sys.dm_os_memory_nodes, sys.dm_os_performance_counters.

Usage           Query requires no persisted tables to use.  Output sent to UI by default.  
                Output designed to minimize volume of data returned.  However, the format minimize analytic flexibility in excel.
                Excel sparklines are great for this analysis.

Author          John Kauffman, the Jolly DBA

Version         2015-09-25 1.0 - Initial attempt

------------------------------------------------------------------------------------------------
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


DECLARE @LOOP_COUNT            INT = 20  -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS INT = 1
DECLARE @UNIT_OF_MEASURE       NCHAR(2) = 'gb' -- OPTIONS ARE 'KB', 'MB', 'GB'

----------------------------------------------------------------------------
/* PREP WORK*/
set nocount on
declare @total_time int= @loop_count * @LOOP_INTERVAL_SECONDS

select 'Results in ' + cast(@total_time as varchar) + ' seconds.  Check messages tab for progress.'

raiserror('   |--------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin prep work', 10, 1) with nowait

declare @batch_counter       int      = 1
declare @original_loop_value int      = @loop_count
declare @now                 datetime 
declare @batch_date          date     

IF OBJECT_ID(N'TEMPDB..#server_and_process') IS NOT NULL DROP TABLE #server_and_process

CREATE TABLE #server_and_process
(
      batch_id                    int
    , batch_datetime              datetime
    , batch_date                  date
    , max_worker_threads          int
    , total_physical_memory       decimal(38,3)
    , free_physical_memory        decimal(38,3)
    , used_physical_memory        decimal(38,3)
    , total_page_file             decimal(38,3)
    , free_page_file              decimal(38,3)
    , used_page_file              decimal(38,3)
    , system_cache                decimal(38,3)
    , kernel_paged_pool           decimal(38,3)
    , kernel_nonpaged_pool        decimal(38,3)
    , committed_memory_mgr        decimal(38,3)
    , committed_memory_mgr_target decimal(38,3)
    , committed_threads           decimal(38,3)
    , physical_memory_in_use      decimal(38,3)
    , large_page_alloc            decimal(38,3)
    , working_set                 decimal(38,3)
    , VAS_reserved                decimal(38,3)
    , VAS_committed               decimal(38,3)
    , avail_commit_limit          decimal(38,3)
    , locked_page_alloc           decimal(38,3)
    , max_memory_worker_threads   decimal(38,3)
    , memory_utilization          decimal(6,2)
    , page_fault_count            bigint
    , physical_memory_low         bit
    , virtual_memory_low          bit
    , committed_direct_OS_alloc   decimal(38,3)
)

IF OBJECT_ID(N'TEMPDB..#memory_nodes') IS NOT NULL DROP TABLE #memory_nodes

CREATE TABLE #memory_nodes
(     batch_id                   int
    , batch_datetime             datetime
    , batch_date                 date
    , memory_node_id             int
    , VAS_reserved               decimal(38,3)
    , VAS_committed              decimal(38,3)
    , pages                      decimal(38,3)
    , shared_memory_reserved     decimal(38,3)
    , shared_memory_committed    decimal(38,3)
    , foreign_committed          decimal(38,3)
    , locked_page_alloc          decimal(38,3)
)

DECLARE @Counter_Prefix NVARCHAR(30)
SET @Counter_Prefix = CASE WHEN @@SERVICENAME = 'MSSQLSERVER'
                          THEN 'SQLServer:' ELSE 'MSSQL$' + @@SERVICENAME + ':' END ;




IF OBJECT_ID(N'TEMPDB..#history_to_pivot') IS NOT NULL DROP TABLE #history_to_pivot

create table #history_to_pivot
( batch_id int
, scope nvarchar(100)
, node  tinyint 
, field nvarchar(128)
, value decimal(38, 3)
)

IF OBJECT_ID(N'TEMPDB..#history_to_pivot_other') IS NOT NULL DROP TABLE #history_to_pivot_other

create table #history_to_pivot_other
( batch_id int
, scope nvarchar(100)
, node smallint
, field nvarchar(128)
, value decimal(38, 3)
)

/* persist explanations of the fields.  meant to be expanded with increased understanding*/

IF OBJECT_ID(N'TEMPDB..#memory_fields_with_explanations') IS NOT NULL DROP TABLE #memory_fields_with_explanations
 
CREATE TABLE #memory_fields_with_explanations
(
      scope              nvarchar(100)
    , is_kb              bit
    , unit_of_measure    varchar(20)
    , column_order       smallint
    , output_column      nvarchar(128)
    , source_dmv         nvarchar(128)
    , source_column      nvarchar(128)
    , bol_description    nvarchar(4000)
    , equivalent_values  nvarchar(4000)
)

begin
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 0 ,'physical_memory'                , 'dm_os_sys_info'      , 'physical_memory_kb'                 , 'Specifies the total amount of physical memory on the machine.', 'resource monitor - in use + modified; sys.dm_os_sys_memory.total_physical_memory_kb' 
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 1 ,'total_physical_memory'          , 'dm_os_sys_memory'    , 'total_physical_memory_kb'           , 'Total size of physical memory available to the operating system', 'resource monitor - in use + modified; sys.dm_os_sys_info.physical_memory_kb'
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 2 ,'free_physical_memory'           , 'dm_os_sys_memory'    , 'available_physical_memory_kb'       , 'Size of physical memory available, in KB.', 'Task Manager - Available;  Resource Monitor - Free + Standby'
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 3 ,'used_physical_memory'           , 'dm_os_sys_memory'    , 'calculated'                         , 'sys.dm_os_sys_memory.total_physical_memory_kb - sys.dm_os_sys_memory.available_physical_memory_kb', 'Task Manager - In use'
                                                                                                                                                                                                                     
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 4 ,'total_page_file'                , 'dm_os_sys_memory'    , 'calculated'                         , 'total_page_file_kb - total_physical_memory_kb.  Size of page file', ''
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 5 ,'free_page_file'                 , 'dm_os_sys_memory'    , 'calculated'                         , 'available_page_file_kb - available_physical_memory_kb Total amount of page file thatis not being used, in KB.', 'Task Manager - Committed (1 of 2)'
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 6 ,'used_page_file'                 , 'dm_os_sys_memory'    , 'calculated'                         , 'total_page_file_kb - m.available_page_file_kb  - m.available_physical_memory_kb', ''
                                                                                                                                                                                                                     
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 7 ,'system_cache'                   , 'dm_os_sys_memory'    , 'system_cache_kb'                    , 'Total amount of system cache memory, in KB.', 'Task Manager - Cached'
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 8 ,'kernel_paged_pool'              , 'dm_os_sys_memory'    , 'kernel_paged_pool_kb'               , 'Total amount of the paged kernel pool, in KB.', 'Task Manager - Paged pool'
   insert into #memory_fields_with_explanations select  'Server'                                   , 1 , 'kb'   , 9 ,'kernel_nonpaged_pool'           , 'dm_os_sys_memory'    , 'kernel_nonpaged_pool_kb'            , 'Total amount of the nonpaged kernel pool, in KB', 'Task Manager - Non-paged pool'
                                                                                                                                                                                                                     
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 1  ,'virtual_memory'                , 'dm_os_sys_info'      , 'virtual_memory_kb'                  , 'Specifies the total amount of virtual address space available to the process in user mode.', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 4  ,'committed_memory_mgr'          , 'dm_os_sys_info'      , 'committed_kb'                       , 'Represents the committed memory in the memory manager. Does not include reserved memory in the memory manager.', 'Perfmon counter - Total Server Memory (KB)'
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 11 ,'committed_memory_mgr_target'   , 'dm_os_sys_info'      , 'committed_target_kb'                , 'Max Server Memory setting.   Represents the amount of memory, that can be consumed by SQL Server memory manager. The target amount is calculated using a variety of inputs like:•the current state of the system including its load •the memory requested by current processes •the amount of memory installed on the computer •configuration parameters   If committed_target_kb is larger than committed_kb, the memory manager will try to obtain additional memory. If committed_target_kb is smaller than committed_kb, the memory manager will try to shrink the amount of memory committed. The committed_target_kb always includes stolen and reserved memory.', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 5  ,'committed_threads'             , 'calculated'          , 'calculated'                                   , 'Represents the amount of memory taken by existing threads.  formula based on (select count(1) from sys.dm_os_threads) * stack_size_in_bytes ', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 6  ,'committed_direct_OS_alloc'     , 'calculated'          , 'calculated'                                   , 'Represents the amount of memory taken by the process, other than for internal memory manager allocations and thread management.  Includes linked servers, extended stored procs, sp_OA calls.  calculated as VAS_committed - (committed for memory manager + committed for threads) ', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 0  ,'visible_target'                , 'dm_os_sys_info'      , 'visible_target_kb'                  , 'Is the same as committed_target_kb.', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 7  ,'physical_memory_in_use'        , 'dm_os_process_memory', 'physical_memory_in_use_kb'          , 'Indicates the process working set in KB, as reported by operating system, as well as tracked allocations by using large page APIs. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 9  ,'large_page_alloc'              , 'dm_os_process_memory', 'large_page_allocations_kb'          , 'Specifies physical memory allocated by using large page APIs.', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 8  ,'working_set'                   , 'calculated'          , 'working set'                        , 'dm_os_process_memory.physical_memory_in_use_kb - dm_os_process_memory.large_page_allocations_kb', 'Resource Monitor - Working Set' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 2  ,'VAS_reserved'                  , 'dm_os_process_memory', 'VAS_reserved_kb'                    , 'Indicates the total amount of virtual address space reserved by the process. ', ''   
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 3  ,'VAS_committed'                 , 'dm_os_process_memory', 'VAS_committed_kb'                   , 'Indicates the amount of reserved virtual address space that has been committed or mapped to physical pages.', 'Resource Monitor - Comitted (KB)'   
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 1  ,'avail_commit_limit'            , 'dm_os_process_memory', 'available_commit_limit_kb'          , 'Indicates the amount of memory that is available to be committed by the process. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 10 ,'locked_page_alloc'             , 'dm_os_process_memory', 'locked_page_allocations_kb'         , 'Specifies memory pages locked in memory. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 1  ,'VAS_available'                 , 'dm_os_process_memory', 'virtual_address_space_available_kb' , 'Indicates the amount of virtual address space that is currently free. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 1 , 'kb'   , 0  ,'Total_VAS'                     , 'dm_os_process_memory', 'total_virtual_address_space_kb'     , 'Indicates the total size of the user mode part of the virtual address space. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'bytes', 1  ,'stack_size'                    , 'dm_os_sys_info'      , 'stack_size_in_bytes'                , 'Specifies the size of the call stack for each thread created by SQL Server', 'Each thread consumes this much memory, outside the value set by max server memory.  multiply by Maximum worker threads to get total memory needed for threading, where max workers = (512+ (<# of processors> -4) * 16)'
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'count', 1  ,'max_worker_threads'            , 'calcluated'          , 'calcluated'                                   , 'max workers = (512+ (si.cpu_count -4) * 16)', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'kb'   , 1  ,'max_memory_for_worker_threads' , 'calcluated'          , 'calcluated'                                   , 'max workers * stack_size_in_bytes/1024.0/1024', ''
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'pct'  , 1  ,'memory_utilization'            , 'dm_os_process_memory', 'memory_utilization_percentage'      , 'Specifies the percentage of committed memory that is in the working set. ', '' 
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'count', 1  ,'page_fault_count'              , 'dm_os_process_memory', 'page_fault_count'                   , 'Indicates the number of page faults that are incurred by the SQL Server process.', ''    
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'bit'  , 1  ,'virtual_memory_low'            , 'dm_os_process_memory', 'process_virtual_memory_low'         , 'Indicates that low virtual memory condition has been detected.', ''    
   insert into #memory_fields_with_explanations select  'Process'                                  , 0 , 'bit'  , 1  ,'physical_memory_low'           , 'dm_os_process_memory', 'process_physical_memory_low'        , 'Indicates that the process is responding to low physical memory notification.', ''    
   /* node-level metrics*/

   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'VAS_reserved'                  , 'dm_os_memory_nodes'  , 'virtual_address_space_reserved_kb'      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'VAS_committed_kb'              , 'dm_os_memory_nodes'  , 'virtual_address_space_committed_kb '      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'pages_kb'                      , 'dm_os_memory_nodes'  , 'pages_kb                           '      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'shared_memory_reserved_kb'     , 'dm_os_memory_nodes'  , 'shared_memory_reserved_kb          '      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'shared_memory_committed_kb'    , 'dm_os_memory_nodes'  , 'shared_memory_committed_kb         '      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'foreign_committed_kb'          , 'dm_os_memory_nodes'  , 'foreign_committed_kb               '      , '', ''
   insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'locked_page_alloc_kb'          , 'dm_os_memory_nodes'  , 'locked_page_allocations_kb         '      , '', ''

   --insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'kb'   , 0  ,'Buffer Pool Memory'            , 'sys.dm_os_performance_counters'  , 'calculated - Buffer Node::Database Pages * 8'      , '', ''
   --insert into #memory_fields_with_explanations select  'Node'                                     , 1 , 'second'   , 0  ,'Page life expectancy'      , 'sys.dm_os_performance_counters'  , 'Buffer Node::Page Life Expectancy'                 , '', ''
                                                                                                                                                                                                   
                                                                                                
   /* from perfmon counters*/
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 0 , 'second', 1  , 'Page life expectancy'         , 'sys.dm_os_performance_counters', 'Buffer Manager::Page life expectancy'         , 'Amount of time a page is expected to stay in memory.', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 2  , 'Target Server Memory'         , 'sys.dm_os_performance_counters', 'Memory Manager::Target Server Memory (KB)'    , '', 'dm_os_sys_info.committed_target_kb'
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 3  , 'Target Buffer Pool'           , 'sys.dm_os_performance_counters', 'calculated - Buffer Manager::Target Pages * 8' , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 4  , 'Total Server Memory'          , 'sys.dm_os_performance_counters', 'Memory Manager::Total Server Memory (KB)'     , '', 'dm_os_sys_info.committed_kb.  max server memory configuration value'
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 5  , 'Buffer Pool Memory'           , 'sys.dm_os_performance_counters', 'Memory Manager::Database Cache Memory (KB)'   , 'Specifies the amount of memory the server is currently using for the database pages cache.', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 6  , 'Free Memory'                  , 'sys.dm_os_performance_counters', 'Memory Manager::Free Memory (KB)'             , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr'                        , 1 , 'kb'    , 7  , 'Stolen Server Memory'         , 'sys.dm_os_performance_counters', 'Memory Manager::Stolen Server Memory (KB)'    , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 8  , 'Plan Cache Memory'            , 'sys.dm_os_performance_counters', 'calculated - Plan Cache::counter_name = ''Cache Pages'' and instance_name = ''_Total'''                   , '', 'can separate out for different subsets (where instance_name <> total'
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 9  , 'Connection Memory'            , 'sys.dm_os_performance_counters', 'Memory Manager::Connection Memory (KB)'       , 'Specifies the total amount of dynamic memory the server is using for maintaining connections.', ''  
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 10 , 'Lock Memory'                  , 'sys.dm_os_performance_counters', 'Memory Manager::Lock Memory (KB)'             , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 11 , 'Optimizer Memory'             , 'sys.dm_os_performance_counters', 'Memory Manager::Optimizer Memory (KB)'        , 'Specifies the total amount of dynamic memory the server is using for query optimization.', ''                                                
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 12 , 'Dynamic SQL Memory'           , 'sys.dm_os_performance_counters', 'Memory Manager::SQL Cache Memory (KB)'        , 'Specifies the total amount of dynamic memory the server is using for the dynamic SQL cache.', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 13 , 'Log Pool Memory'              , 'sys.dm_os_performance_counters', 'Memory Manager::Log Pool Memory (KB)'         , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen'               , 1 , 'kb'    , 18 , 'Stolen Memory - Other'        , 'sys.dm_os_performance_counters', 'calculated:  stolen - (plan cache + connection + lock + optimizer + dynamic + log pool  + granted workspace)'        , '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen - Mem Grants'  , 1 , 'kb'    , 14 , 'Granted Workspace Memory'     , 'sys.dm_os_performance_counters', 'Memory Manager::Granted Workspace Memory (KB)', '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen - Mem Grants'  , 1 , 'kb'    , 15 , 'Reserved Workspace Memory'    , 'sys.dm_os_performance_counters', 'Memory Manager::Reserved Server Memory (KB)'  , 'Indicates the amount of memory the server has reserved for future usage. This counter shows the current unused amount of memory initially granted that is shown in Granted Workspace Memory (KB).', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen - Mem Grants'  , 1 , 'kb'    , 16 , 'Maximum Workspace Memory'     , 'sys.dm_os_performance_counters', 'Memory Manager::Maximum Workspace Memory (KB)', '', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen - Mem Grants'  , 0 , 'count' , 17 , 'Memory Grants Outstanding'    , 'sys.dm_os_performance_counters', 'Memory Manager::Memory Grants Outstanding'    , 'Currently executing queries, that had been in pending', ''
   insert into #memory_fields_with_explanations select  'Process - Mem Mgr - Stolen - Mem Grants'  , 0 , 'count' , 18 , 'Memory Grants Pending'        , 'sys.dm_os_performance_counters', 'Memory Manager::Memory Grants Pending'        , 'Queries waiting for enough memory to begin executing', ''
end  /* persist explanations of the fields.  meant to be expanded with increased understanding*/


raiserror('|- end   prep work', 10, 1) with nowait
raiserror('|--------------------------------------------------------------', 10, 1) with nowait

raiserror('|- begin loop', 10, 1) with nowait

while @loop_count >= 1
   begin

      set @now = getdate()
      set @batch_date = cast(@now as date)

/* server and process level info*/
      insert into #server_and_process
         select 
           @batch_counter                        as batch_id
         , @now                                  as batch_datetime
         , @batch_date                           as batch_date
         , 512+ ((si.cpu_count -4) * 16)         as max_worker_threads
         , m.total_physical_memory_kb    
         , m.available_physical_memory_kb
         , cast((m.total_physical_memory_kb - m.available_physical_memory_kb) as decimal(38, 3)) as used_physical_memory_kb
         , cast((m.total_page_file_kb - m.total_physical_memory_kb          ) as decimal(38, 3)) as total_page_file_kb   
         , cast((m.available_page_file_kb  - m.available_physical_memory_kb ) as decimal(38, 3)) as available_page_file_kb
         , cast((m.total_page_file_kb - m.total_physical_memory_kb          ) - (m.available_page_file_kb  - m.available_physical_memory_kb ) as decimal(38, 3)) as used_page_file_kb
         , m.system_cache_kb             
         , m.kernel_paged_pool_kb        
         , m.kernel_nonpaged_pool_kb     
         , si.committed_kb          as committed_for_memory_manager_kb         
         , si.committed_target_kb   as committed_target_for_memory_manager_kb
         --, cast((select count(1) from sys.dm_os_threads) * stack_size_in_bytes/1024.0 as decimal(38, 3))as committed_for_threads_kb
         , (SELECT CAST(SUM(STACK_BYTES_COMMITTED)/1024.0  AS DECIMAL(38, 2)) FROM SYS.dm_os_threads) as committed_for_threads_kb
         , pm.physical_memory_in_use_kb             
         , pm.large_page_allocations_kb     
         , pm.physical_memory_in_use_kb - pm.large_page_allocations_kb as Working_Set_kb
         , pm.virtual_address_space_reserved_kb     as VAS_reserved_kb
         , pm.virtual_address_space_committed_kb    as VAS_committed_kb
         , pm.available_commit_limit_kb         
         , pm.locked_page_allocations_kb        
         , cast((512+ ((si.cpu_count -4) * 16)) * si.stack_size_in_bytes /1024.0 as decimal(38, 3))  as max_memory_for_worker_threads_kb
         , pm.memory_utilization_percentage
         , pm.page_fault_count                       
         , pm.process_physical_memory_low            
         , pm.process_virtual_memory_low  
         , pm.virtual_address_space_committed_kb - si.committed_kb - (SELECT CAST(SUM(STACK_BYTES_COMMITTED)/1024.0  AS DECIMAL(38, 2)) FROM SYS.dm_os_threads)    as committed_for_direct_OS_allocation_kb           
         from sys.dm_os_sys_memory m
         cross join sys.dm_os_sys_info si
         cross join sys.dm_os_process_memory pm

/* node-level info*/
      insert into #memory_nodes
         SELECT
               @batch_counter                      as batch_id
             , @now                                as batch_datetime
             , @batch_date                         as batch_date
             , memory_node_id                      as memory_node_id
             , virtual_address_space_reserved_kb   as VAS_reserved   
             , virtual_address_space_committed_kb  as VAS_committed   
             , pages_kb                            as pages   
             , shared_memory_reserved_kb           as shared_memory_reserved   
             , shared_memory_committed_kb          as shared_memory_committed   
             , foreign_committed_kb                as foreign_committed   
             , locked_page_allocations_kb          as locked_page_alloc   
         from sys.dm_os_memory_nodes


         IF OBJECT_ID(N'TEMPDB..#perfmon_counters') is not null DROP TABLE #perfmon_counters
         IF OBJECT_ID(N'TEMPDB..#memory') is not null DROP TABLE #memory

         SELECT ltrim(rtrim(counter_name)) as COUNTER_NAME, CNTR_VALUE
         INTO #perfmon_counters
         FROM sys.dm_os_performance_counters
         WHERE 
            ( OBJECT_NAME = @counter_prefix + 'Buffer Manager'     AND counter_name = 'Page life expectancy')
         OR ( OBJECT_NAME = @counter_prefix + 'Buffer Manager'     AND counter_name = 'TARGET PAGES')
         OR ( OBJECT_NAME = @counter_prefix + 'Memory Manager'     AND counter_name = 'Memory Grants Pending')
         OR ( OBJECT_NAME = @counter_prefix + 'Memory Manager'     AND counter_name = 'Memory Grants Outstanding')
         OR ( OBJECT_NAME = @counter_prefix + 'Memory Manager'     AND counter_name like '%(kb)%' )


         insert into #perfmon_counters
         select 'Plan Cache Memory (KB)'
         , cntr_value * 8
         FROM sys.dm_os_performance_counters
         WHERE object_name like '%Plan Cache%' and counter_name = 'Cache Pages' and instance_name = '_Total'


         insert into #perfmon_counters
         select 'Target Buffer Pool (KB)'
         , cntr_value * 8
         FROM #perfmon_counters
         WHERE counter_name = 'target pages'

         delete from #perfmon_counters where counter_name = 'target pages'


         insert into #perfmon_counters
         select 'Stolen Memory - Other (KB)', (select cntr_value from #perfmon_counters where counter_name = 'Stolen Server Memory (KB)') - sum(cntr_value)
         from #perfmon_counters   
         where counter_name in (   'Connection Memory (KB)'
                                 , 'Granted Workspace Memory (KB)'
                                 , 'Lock Memory (KB)'
                                 , 'Optimizer Memory (KB)'
                                 , 'SQL Cache Memory (KB)'
                                 , 'Log Pool Memory (KB)'
                                 , 'Plan Cache Memory (KB)')

         update #perfmon_counters set counter_name = 'Buffer Pool Memory (KB)' where counter_name = 'Database Cache Memory (KB)'
         update #perfmon_counters set counter_name = 'Reserved Workspace Memory (KB)' where counter_name = 'Reserved Server Memory (KB)'
         update #perfmon_counters set counter_name = 'Dynamic SQL Memory (KB)' where counter_name = 'SQL Cache Memory (KB)'


      /* PUT THE KB-BASED VALUES IN #HISTORY_TO_PIVOT.
      DON'T PUT FIELDS WITH OTHER UNITS OF MEASURE, BECAUSE OF THE KB-MB OR KB-GB COMING UP.
      hAVE TO PUT THE REST IN A STORAGE TABLE TO GO INTO #HISTORY_TO_PIVOT LATER*/

      insert into #history_to_pivot
         select @batch_counter
              , scope
              , null
              , replace(counter_name, '(KB)', '')
              , cntr_value
         from #perfmon_counters pc
         left join #memory_fields_with_explanations mfe on mfe.output_column = replace(counter_name, ' (KB)', '')
         where counter_name like '%(KB)%'

      insert into #history_to_pivot_other
         select @batch_counter
              , scope
              , null
              , counter_name
              , cntr_value
         from #perfmon_counters
         left join #memory_fields_with_explanations mfe on mfe.output_column = counter_name
         where counter_name not like '%(KB)%'


      waitfor delay @LOOP_INTERVAL_SECONDS

      raiserror('   |- loop %d of %d complete', 10, 1, @batch_counter, @original_loop_value) with nowait

      set @LOOP_COUNT = @LOOP_COUNT - 1
      set @batch_counter = @batch_counter + 1

   end -- while @loop_count >= 1

raiserror('|- end   loop', 10, 1) with nowait
raiserror('|--------------------------------------------------------------', 10, 1) with nowait


----------------------------------------------------------------------------------------------------

raiserror('|- begin output', 10, 1) with nowait
raiserror('   |- begin load #history_to_pivot', 10, 1) with nowait


      insert into #history_to_pivot select batch_id, 'Server',  null, 'total_physical_memory'        , total_physical_memory       from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'free_physical_memory'         , free_physical_memory        from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'used_physical_memory'         , used_physical_memory        from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'total_page_file'              , total_page_file             from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'free_page_file'               , free_page_file              from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'used_page_file'               , used_page_file              from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'system_cache'                 , system_cache                from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'kernel_paged_pool'            , kernel_paged_pool           from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Server',  null, 'kernel_nonpaged_pool'         , kernel_nonpaged_pool        from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'committed_memory_mgr'        , committed_memory_mgr        from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'committed_target_memory_mgr' , committed_memory_mgr_target from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'committed_threads'           , committed_threads           from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'physical_memory_in_use'      , physical_memory_in_use      from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'large_page_alloc'            , large_page_alloc            from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'working_set'                 , Working_Set                 from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'VAS_reserved'                , VAS_reserved                from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'VAS_committed'               , VAS_committed               from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'avail_commit_limit'          , avail_commit_limit          from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'locked_page_alloc'           , locked_page_alloc           from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'max_memory_worker_threads'   , max_memory_worker_threads   from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null,  'committed_direct_OS_alloc'   , committed_direct_OS_alloc   from #server_and_process 
                                                             
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'VAS_reserved_kb'                , VAS_reserved                  from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'VAS_committed_kb'               , VAS_committed                 from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'pages_kb'                       , pages                         from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'shared_memory_reserved_kb'      , shared_memory_reserved        from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'shared_memory_committed_kb'     , shared_memory_committed       from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'foreign_committed_kb'           , foreign_committed             from #memory_nodes 
      insert into #history_to_pivot select batch_id, 'Node', memory_node_id, 'locked_page_alloc_kb'           , locked_page_alloc             from #memory_nodes 



IF @UNIT_OF_MEASURE = 'MB'
   BEGIN
      update #history_to_pivot set value = value /1024.0
   END

IF @UNIT_OF_MEASURE = 'GB'
   BEGIN
      update #history_to_pivot set value = value /1024.0/1024
   END



/* include the fields that aren't measured in bytes*/
      insert into #history_to_pivot select batch_id, 'Process', null, 'max_worker_threads'     , cast(max_worker_threads     as decimal(38, 3)) from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null, 'memory_utilization '    , cast(memory_utilization     as decimal(38, 3)) from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null, 'page_fault_count'       , cast(page_fault_count       as decimal(38, 3)) from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null, 'virtual_memory_low'     , cast(virtual_memory_low     as decimal(38, 3)) from #server_and_process 
      insert into #history_to_pivot select batch_id, 'Process', null, 'physical_memory_low'    , cast(physical_memory_low    as decimal(38, 3)) from #server_and_process 


      insert into #history_to_pivot select * from #history_to_pivot_other
 

                                                              
raiserror('   |- end   load #history_to_pivot', 10, 1) with nowait
raiserror('   |- begin calculate pct diffs', 10, 1) with nowait


/* calculate percent diffs for each value (comparing min and max values collected*/

IF OBJECT_ID(N'TEMPDB..#pct_diff') IS NOT NULL DROP TABLE #pct_diff
 
CREATE TABLE #pct_diff
(
      scope     nvarchar(100)
    , node   smallint
    , field     nvarchar(128)
    , avg_value decimal(38,3)
    , min_value decimal(38,3)
    , max_value decimal(38,3)
    , pct_diff  decimal(38,6)
)
  
insert into #pct_diff
   select scope, node, field, avg(value) as avg_value, min(value) as min_value, max(value) as max_value
   , case when min(value) = max(value) then 0
          when min(value) = 0 and max(value) <> 0 then 999999999  else (max(value) - min(value)) / min(value) * 100 end as pct_diff
   from #history_to_pivot
   group by scope, field, node


declare @min_datetime datetime = (select top 1 batch_datetime from #memory_nodes where batch_id = 1)
declare @max_datetime datetime = (select top 1 batch_datetime from #memory_nodes where batch_id = @original_loop_value)

raiserror('   |- end   calculate pct diffs', 10, 1) with nowait

 /* start pivot*/
raiserror('   |- begin generating pivot for floats', 10, 1) with nowait

 if not exists (select 1 from #history_to_pivot)
    begin
       select 'No changes in any selected counters.  Cannot pivot.  Exiting.'
       return
    end 
else 
   begin --pivot logic
      if object_id(N'tempdb..#cols') is not null drop table #cols
      if object_id(N'tempdb..##pivoted_output') is not null drop table ##pivoted_output

      select distinct batch_id, field   
      into #cols
      from #history_to_pivot
      order by field, batch_id

      /* build csv list. */
      declare  @cols as nvarchar(max)
            , @sql as nvarchar(max)
            , @parm_definition nvarchar(500)

      set @cols = stuff(
                        (select N',' + quotename(batch_id) as [text()]
                        from (select distinct batch_id from #history_to_pivot) as y
                        order by batch_id
                        for xml path('')), 1, 1, '')

      /* build pivot*/

      set @sql = N'         select * into ##pivoted_output from 
                     (select 
                          h.scope
                        , h.node
                        , h.field
                        , h.value
                        , h.batch_id
                     from #history_to_pivot h
                     join #cols   c on c.batch_id = h.batch_id and c.field = h.field) as d
                  pivot (max(VALUE) for batch_id in (' + @cols + ')) as p   ;'

         print @sql
         exec sp_executesql @sql


      end --pivot logic
raiserror('   |- end   generating pivot for floats', 10, 1) with nowait


raiserror('   |- begin return final output', 10, 1) with nowait
select 
 @min_datetime as min_datetime
, @max_datetime as max_datetime

select  case when is_kb = 1 then lower(@UNIT_OF_MEASURE) else mfe.unit_of_measure end as unit_of_measure 
, po.*
, pct.min_value
, pct.avg_value
, pct.max_value    
, pct.pct_diff
, mfe.source_dmv
, mfe.source_column
, mfe.bol_description, mfe.equivalent_values
from ##pivoted_output                       po
left join #pct_diff                        pct on  pct.field = po.field and pct.scope = po.scope and coalesce(pct.node, -1) = coalesce(po.node, -1)
left join #memory_fields_with_explanations mfe on mfe.output_column = po.field and pct.scope = po.scope
order by case when po.SCOPE = 'server' then 0 else 1 end, po.scope, column_order, po.field, po.node


raiserror('   |- end   return final output', 10, 1) with nowait
raiserror('|- end   output', 10, 1) with nowait

--if object_id(N'tempdb..##pivoted_output') is not null drop table ##pivoted_output

