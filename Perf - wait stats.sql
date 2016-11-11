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

DECLARE @SHOW_HISTORICAL_SNAPSHOT   BIT         =  1  -- PULLS DATA ACCUMULATED SINCE SERVER RESTART.

DECLARE @CALC_INTERVAL_DIFFS        BIT         =  1 -- COMPARES CURRENT DATA TO PRIOR DATA AND CALCULATES DIFFS IN A LOOP.
DECLARE @LOOP_COUNT                 INT         =  3 -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS      INT         =  1
                                    
/* FILTER PARAMETERS*/  
DECLARE @TOP_N                      INT         =  20 -- RETURN TOP N BY WAIT_TIME_MS descending.  applied after next two filters
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
      insert into #excluded_wait_types select 'HADR_WORK_QUEUE'
      insert into #excluded_wait_types select 'HADR_LOGCAPTURE_WAIT'
      insert into #excluded_wait_types select 'HADR_NOTIFICATION_DEQUEUE'
      insert into #excluded_wait_types select 'HADR_CLUSAPI_CALL'
      insert into #excluded_wait_types select 'HADR_TIMER_TASK'

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
      insert into #excluded_wait_types select 'SLEEP_DBSTARTUP'

SLEEP_DBSTARTUP
   end --if @EXCLUDE_USELESS_WAIT_TYPES = 1
----------------------------------------------------------------------------
/* PREP WORK*/

declare @total_time int= @loop_count * @LOOP_INTERVAL_SECONDS

select 'Results in ' + cast(@total_time as varchar) + ' seconds.  Check messages tab for progress.'

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
    , category_name               nvarchar(128)
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
    , category_name               nvarchar(128)
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

IF OBJECT_ID(N'TEMPDB..#wait_category') IS NOT NULL DROP TABLE #wait_category
 
create table #wait_category 
(category_name nvarchar(128)
, wait_type nvarchar(128))

insert into #wait_category select 'Other', 'ABR'
insert into #wait_category select 'SQLCLR', 'ASSEMBLY_LOAD'
insert into #wait_category select 'Buffer I/O', 'ASYNC_DISKPOOL_LOCK'
insert into #wait_category select 'Buffer I/O', 'ASYNC_IO_COMPLETION'
insert into #wait_category select 'Network I/O', 'ASYNC_NETWORK_IO'
insert into #wait_category select 'Backup', 'BACKUP'
insert into #wait_category select 'Backup', 'BACKUP_CLIENTLOCK'
insert into #wait_category select 'Backup', 'BACKUP_OPERATOR'
insert into #wait_category select 'Backup', 'BACKUPBUFFER'
insert into #wait_category select 'Backup', 'BACKUPIO'
insert into #wait_category select 'Backup', 'BACKUPTHREAD'
insert into #wait_category select 'Other', 'BAD_PAGE_PROCESS'
insert into #wait_category select 'Other', 'BROKER_CONNECTION_RECEIVE_TASK'
insert into #wait_category select 'Other', 'BROKER_ENDPOINT_STATE_MUTEX'
insert into #wait_category select 'Idle', 'BROKER_EVENTHANDLER'
insert into #wait_category select 'Other', 'BROKER_INIT'
insert into #wait_category select 'Other', 'BROKER_MASTERSTART'
insert into #wait_category select 'Idle', 'BROKER_RECEIVE_WAITFOR'
insert into #wait_category select 'Other', 'BROKER_REGISTERALLENDPOINTS'
insert into #wait_category select 'Other', 'BROKER_SHUTDOWN'
insert into #wait_category select 'Other', 'BROKER_TASK_STOP'
insert into #wait_category select 'Idle', 'BROKER_TRANSMITTER'
insert into #wait_category select 'Other', 'BUILTIN_HASHKEY_MUTEX'
insert into #wait_category select 'Other', 'CHECK_PRINT_RECORD'
insert into #wait_category select 'Idle', 'CHECKPOINT_QUEUE'
insert into #wait_category select 'Idle', 'CHKPT'
insert into #wait_category select 'Idle', 'CLR_AUTO_EVENT'
insert into #wait_category select 'SQLCLR', 'CLR_CRST'
insert into #wait_category select 'SQLCLR', 'CLR_JOIN'
insert into #wait_category select 'Idle', 'CLR_MANUAL_EVENT'
insert into #wait_category select 'SQLCLR', 'CLR_MEMORY_SPY'
insert into #wait_category select 'SQLCLR', 'CLR_MONITOR'
insert into #wait_category select 'SQLCLR', 'CLR_RWLOCK_READER'
insert into #wait_category select 'SQLCLR', 'CLR_RWLOCK_WRITER'
insert into #wait_category select 'SQLCLR', 'CLR_SEMAPHORE'
insert into #wait_category select 'SQLCLR', 'CLR_TASK_START'
insert into #wait_category select 'SQLCLR', 'CLRHOST_STATE_ACCESS'
insert into #wait_category select 'Memory', 'CMEMTHREAD'
insert into #wait_category select 'CPU', 'CPU'
insert into #wait_category select 'Other', 'CURSOR'
insert into #wait_category select 'Other', 'CURSOR_ASYNC'
insert into #wait_category select 'Parallelism', 'CXPACKET'
insert into #wait_category select 'Other', 'DAC_INIT'
insert into #wait_category select 'Other', 'DBCC_COLUMN_TRANSLATION_CACHE'
insert into #wait_category select 'Other', 'DBMIRROR_DBM_EVENT'
insert into #wait_category select 'Other', 'DBMIRROR_DBM_MUTEX'
insert into #wait_category select 'Other', 'DBMIRROR_EVENTS_QUEUE'
insert into #wait_category select 'Network I/O', 'DBMIRROR_SEND'
insert into #wait_category select 'Other', 'DBMIRROR_WORKER_QUEUE'
insert into #wait_category select 'Other', 'DBMIRRORING_CMD'
insert into #wait_category select 'Other', 'DBTABLE'
insert into #wait_category select 'Latch', 'DEADLOCK_ENUM_MUTEX'
insert into #wait_category select 'Other', 'DEADLOCK_TASK_SEARCH'
insert into #wait_category select 'Other', 'DEBUG'
insert into #wait_category select 'Other', 'DISABLE_VERSIONING'
insert into #wait_category select 'Backup', 'DISKIO_SUSPEND'
insert into #wait_category select 'Other', 'DLL_LOADING_MUTEX'
insert into #wait_category select 'Other', 'DROPTEMP'
insert into #wait_category select 'Transaction', 'DTC'
insert into #wait_category select 'Transaction', 'DTC_ABORT_REQUEST'
insert into #wait_category select 'Transaction', 'DTC_RESOLVE'
insert into #wait_category select 'Network I/O', 'DTC_STATE'
insert into #wait_category select 'Transaction', 'DTC_TMDOWN_REQUEST'
insert into #wait_category select 'Transaction', 'DTC_WAITFOR_OUTCOME'
insert into #wait_category select 'Other', 'DUMP_LOG_COORDINATOR'
insert into #wait_category select 'Other', 'DUMP_LOG_COORDINATOR_QUEUE'
insert into #wait_category select 'Other', 'DUMPTRIGGER'
insert into #wait_category select 'Other', 'EC'
insert into #wait_category select 'Other', 'EE_PMOLOCK'
insert into #wait_category select 'Other', 'EE_SPECPROC_MAP_INIT'
insert into #wait_category select 'Other', 'ENABLE_VERSIONING'
insert into #wait_category select 'Other', 'ERROR_REPORTING_MANAGER'
insert into #wait_category select 'Parallelism', 'EXCHANGE'
insert into #wait_category select 'Parallelism', 'EXECSYNC'
insert into #wait_category select 'Other', 'EXECUTION_PIPE_EVENT_INTERNAL'
insert into #wait_category select 'Other', 'FAILPOINT'
insert into #wait_category select 'Buffer I/O', 'FCB_REPLICA_READ'
insert into #wait_category select 'Buffer I/O', 'FCB_REPLICA_WRITE'
insert into #wait_category select 'SQLCLR', 'FS_GARBAGE_COLLECTOR_SHUTDOWN'
insert into #wait_category select 'Idle', 'FSAGENT'
insert into #wait_category select 'Other', 'FT_RESTART_CRAWL'
insert into #wait_category select 'Other', 'FT_RESUME_CRAWL'
insert into #wait_category select 'Other', 'FULLTEXT GATHERER'
insert into #wait_category select 'Other', 'GUARDIAN'
insert into #wait_category select 'Other', 'HTTP_ENDPOINT_COLLCREATE'
insert into #wait_category select 'Other', 'HTTP_ENUMERATION'
insert into #wait_category select 'Other', 'HTTP_START'
insert into #wait_category select 'Other', 'IMP_IMPORT_MUTEX'
insert into #wait_category select 'Other', 'IMPPROV_IOWAIT'
insert into #wait_category select 'Latch', 'INDEX_USAGE_STATS_MUTEX'
insert into #wait_category select 'Other', 'INTERNAL_TESTING'
insert into #wait_category select 'Other', 'IO_AUDIT_MUTEX'
insert into #wait_category select 'Buffer I/O', 'IO_COMPLETION'
insert into #wait_category select 'Idle', 'KSOURCE_WAKEUP'
insert into #wait_category select 'Other', 'KTM_ENLISTMENT'
insert into #wait_category select 'Other', 'KTM_RECOVERY_MANAGER'
insert into #wait_category select 'Other', 'KTM_RECOVERY_RESOLUTION'
insert into #wait_category select 'Latch', 'LATCH_DT'
insert into #wait_category select 'Latch', 'LATCH_EX'
insert into #wait_category select 'Latch', 'LATCH_KP'
insert into #wait_category select 'Latch', 'LATCH_NL'
insert into #wait_category select 'Latch', 'LATCH_SH'
insert into #wait_category select 'Latch', 'LATCH_UP'
insert into #wait_category select 'Idle', 'LAZYWRITER_SLEEP'
insert into #wait_category select 'Lock', 'LCK_M_BU'
insert into #wait_category select 'Lock', 'LCK_M_IS'
insert into #wait_category select 'Lock', 'LCK_M_IU'
insert into #wait_category select 'Lock', 'LCK_M_IX'
insert into #wait_category select 'Lock', 'LCK_M_RIn_NL'
insert into #wait_category select 'Lock', 'LCK_M_RIn_S'
insert into #wait_category select 'Lock', 'LCK_M_RIn_U'
insert into #wait_category select 'Lock', 'LCK_M_RIn_X'
insert into #wait_category select 'Lock', 'LCK_M_RS_S'
insert into #wait_category select 'Lock', 'LCK_M_RS_U'
insert into #wait_category select 'Lock', 'LCK_M_RX_S'
insert into #wait_category select 'Lock', 'LCK_M_RX_U'
insert into #wait_category select 'Lock', 'LCK_M_RX_X'
insert into #wait_category select 'Lock', 'LCK_M_S'
insert into #wait_category select 'Lock', 'LCK_M_SCH_M'
insert into #wait_category select 'Lock', 'LCK_M_SCH_S'
insert into #wait_category select 'Lock', 'LCK_M_SIU'
insert into #wait_category select 'Lock', 'LCK_M_SIX'
insert into #wait_category select 'Lock', 'LCK_M_U'
insert into #wait_category select 'Lock', 'LCK_M_UIX'
insert into #wait_category select 'Lock', 'LCK_M_X'
insert into #wait_category select 'Logging', 'LOGBUFFER'
insert into #wait_category select 'Logging', 'LOGMGR'
insert into #wait_category select 'Logging', 'LOGMGR_FLUSH'
insert into #wait_category select 'Idle', 'LOGMGR_QUEUE'
insert into #wait_category select 'Logging', 'LOGMGR_RESERVE_APPEND'
insert into #wait_category select 'Memory', 'LOWFAIL_MEMMGR_QUEUE'
insert into #wait_category select 'Other', 'MIRROR_SEND_MESSAGE'
insert into #wait_category select 'Other', 'MISCELLANEOUS'
insert into #wait_category select 'Network I/O', 'MSQL_DQ'
insert into #wait_category select 'Other', 'MSQL_SYNC_PIPE'
insert into #wait_category select 'Transaction', 'MSQL_XACT_MGR_MUTEX'
insert into #wait_category select 'Transaction', 'MSQL_XACT_MUTEX'
insert into #wait_category select 'Other', 'MSQL_XP'
insert into #wait_category select 'Full Text Search', 'MSSEARCH'
insert into #wait_category select 'Network I/O', 'NET_WAITFOR_PACKET'
insert into #wait_category select 'Network I/O', 'OLEDB'
insert into #wait_category select 'Idle', 'ONDEMAND_TASK_QUEUE'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_DT'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_EX'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_KP'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_NL'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_SH'
insert into #wait_category select 'Buffer I/O', 'PAGEIOLATCH_UP'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_DT'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_EX'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_KP'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_NL'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_SH'
insert into #wait_category select 'Buffer Latch', 'PAGELATCH_UP'
insert into #wait_category select 'Other', 'PARALLEL_BACKUP_QUEUE'
insert into #wait_category select 'Other', 'PRINT_ROLLBACK_PROGRESS'
insert into #wait_category select 'Other', 'QNMANAGER_ACQUIRE'
insert into #wait_category select 'Other', 'QPJOB_KILL'
insert into #wait_category select 'Other', 'QPJOB_WAITFOR_ABORT'
insert into #wait_category select 'Other', 'QRY_MEM_GRANT_INFO_MUTEX'
insert into #wait_category select 'Other', 'QUERY_ERRHDL_SERVICE_DONE'
insert into #wait_category select 'Other', 'QUERY_EXECUTION_INDEX_SORT_EVENT_OPEN'
insert into #wait_category select 'Other', 'QUERY_NOTIFICATION_MGR_MUTEX'
insert into #wait_category select 'Other', 'QUERY_NOTIFICATION_SUBSCRIPTION_MUTEX'
insert into #wait_category select 'Other', 'QUERY_NOTIFICATION_TABLE_MGR_MUTEX'
insert into #wait_category select 'Other', 'QUERY_NOTIFICATION_UNITTEST_MUTEX'
insert into #wait_category select 'Other', 'QUERY_OPTIMIZER_PRINT_MUTEX'
insert into #wait_category select 'Other', 'QUERY_REMOTE_BRICKS_DONE'
insert into #wait_category select 'Other', 'QUERY_TRACEOUT'
insert into #wait_category select 'Other', 'RECOVER_CHANGEDB'
insert into #wait_category select 'Other', 'REPL_CACHE_ACCESS'
insert into #wait_category select 'Other', 'REPL_SCHEMA_ACCESS'
insert into #wait_category select 'Buffer I/O', 'REPLICA_WRITES'
insert into #wait_category select 'Other', 'REQUEST_DISPENSER_PAUSE'
insert into #wait_category select 'Idle', 'REQUEST_FOR_DEADLOCK_SEARCH'
insert into #wait_category select 'Idle', 'RESOURCE_QUEUE'
insert into #wait_category select 'Memory', 'RESOURCE_SEMAPHORE'
insert into #wait_category select 'Compilation', 'RESOURCE_SEMAPHORE_MUTEX'
insert into #wait_category select 'Compilation', 'RESOURCE_SEMAPHORE_QUERY_COMPILE'
insert into #wait_category select 'Compilation', 'RESOURCE_SEMAPHORE_SMALL_QUERY'
insert into #wait_category select 'Other', 'SEC_DROP_TEMP_KEY'
insert into #wait_category select 'Other', 'SEQUENTIAL_GUID'
insert into #wait_category select 'Idle', 'SERVER_IDLE_CHECK'
insert into #wait_category select 'Other', 'SHUTDOWN'
insert into #wait_category select 'Idle', 'SLEEP_BPOOL_FLUSH'
insert into #wait_category select 'Idle', 'SLEEP_DBSTARTUP'
insert into #wait_category select 'Idle', 'SLEEP_DCOMSTARTUP'
insert into #wait_category select 'Idle', 'SLEEP_MSDBSTARTUP'
insert into #wait_category select 'Idle', 'SLEEP_SYSTEMTASK'
insert into #wait_category select 'Idle', 'SLEEP_TASK'
insert into #wait_category select 'Idle', 'SLEEP_TEMPDBSTARTUP'
insert into #wait_category select 'Other', 'SNI_CRITICAL_SECTION'
insert into #wait_category select 'Idle', 'SNI_HTTP_ACCEPT'
insert into #wait_category select 'Other', 'SNI_HTTP_WAITFOR_0_DISCON'
insert into #wait_category select 'Other', 'SNI_LISTENER_ACCESS'
insert into #wait_category select 'Other', 'SNI_TASK_COMPLETION'
insert into #wait_category select 'Full Text Search', 'SOAP_READ'
insert into #wait_category select 'Full Text Search', 'SOAP_WRITE'
insert into #wait_category select 'Other', 'SOS_CALLBACK_REMOVAL'
insert into #wait_category select 'Other', 'SOS_DISPATCHER_MUTEX'
insert into #wait_category select 'Other', 'SOS_LOCALALLOCATORLIST'
insert into #wait_category select 'Other', 'SOS_OBJECT_STORE_DESTROY_MUTEX'
insert into #wait_category select 'Other', 'SOS_PROCESS_AFFINITY_MUTEX'
insert into #wait_category select 'Memory', 'SOS_RESERVEDMEMBLOCKLIST'
insert into #wait_category select 'CPU', 'SOS_SCHEDULER_YIELD'
insert into #wait_category select 'Other', 'SOS_STACKSTORE_INIT_MUTEX'
insert into #wait_category select 'Other', 'SOS_SYNC_TASK_ENQUEUE_EVENT'
insert into #wait_category select 'Memory', 'SOS_VIRTUALMEMORY_LOW'
insert into #wait_category select 'Other', 'SOSHOST_EVENT'
insert into #wait_category select 'Other', 'SOSHOST_INTERNAL'
insert into #wait_category select 'Other', 'SOSHOST_MUTEX'
insert into #wait_category select 'Other', 'SOSHOST_RWLOCK'
insert into #wait_category select 'Other', 'SOSHOST_SEMAPHORE'
insert into #wait_category select 'Other', 'SOSHOST_SLEEP'
insert into #wait_category select 'Other', 'SOSHOST_TRACELOCK'
insert into #wait_category select 'Other', 'SOSHOST_WAITFORDONE'
insert into #wait_category select 'SQLCLR', 'SQLCLR_APPDOMAIN'
insert into #wait_category select 'SQLCLR', 'SQLCLR_ASSEMBLY'
insert into #wait_category select 'SQLCLR', 'SQLCLR_DEADLOCK_DETECTION'
insert into #wait_category select 'SQLCLR', 'SQLCLR_QUANTUM_PUNISHMENT'
insert into #wait_category select 'Other', 'SQLSORT_NORMMUTEX'
insert into #wait_category select 'Other', 'SQLSORT_SORTMUTEX'
insert into #wait_category select 'Idle', 'SQLTRACE_BUFFER_FLUSH'
insert into #wait_category select 'Other', 'SQLTRACE_LOCK'
insert into #wait_category select 'Other', 'SQLTRACE_SHUTDOWN'
insert into #wait_category select 'Other', 'SQLTRACE_WAIT_ENTRIES'
insert into #wait_category select 'Other', 'SRVPROC_SHUTDOWN'
insert into #wait_category select 'Other', 'TEMPOBJ'
insert into #wait_category select 'Other', 'THREADPOOL'
insert into #wait_category select 'Other', 'TIMEPRIV_TIMEPERIOD'
insert into #wait_category select 'Idle', 'TRACEWRITE'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_DT'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_EX'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_KP'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_NL'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_SH'
insert into #wait_category select 'Transaction', 'TRAN_MARKLATCH_UP'
insert into #wait_category select 'Transaction', 'TRANSACTION_MUTEX'
insert into #wait_category select 'Memory', 'UTIL_PAGE_ALLOC'
insert into #wait_category select 'Other', 'VIA_ACCEPT'
insert into #wait_category select 'Latch', 'VIEW_DEFINITION_MUTEX'
insert into #wait_category select 'Idle', 'WAIT_FOR_RESULTS'
insert into #wait_category select 'User Waits', 'WAITFOR'
insert into #wait_category select 'Idle', 'WAITFOR_TASKSHUTDOWN'
insert into #wait_category select 'Other', 'WAITSTAT_MUTEX'
insert into #wait_category select 'Other', 'WCC'
insert into #wait_category select 'Other', 'WORKTBL_DROP'
insert into #wait_category select 'Logging', 'WRITELOG'
insert into #wait_category select 'Transaction', 'XACT_OWN_TRANSACTION'
insert into #wait_category select 'Transaction', 'XACT_RECLAIM_SESSION'
insert into #wait_category select 'Transaction', 'XACTLOCKINFO'
insert into #wait_category select 'Transaction', 'XACTWORKSPACE_MUTEX'
insert into #wait_category select 'Other', 'XE_BUFFERMGR_ALLPROCECESSED_EVENT'
insert into #wait_category select 'Other', 'XE_BUFFERMGR_FREEBUF_EVENT'
insert into #wait_category select 'Other', 'XE_DISPATCHER_JOIN'
insert into #wait_category select 'Idle', 'XE_DISPATCHER_WAIT'
insert into #wait_category select 'Other', 'XE_MODULEMGR_SYNC'
insert into #wait_category select 'Other', 'XE_OLS_LOCK'
insert into #wait_category select 'Other', 'XE_SERVICES_MUTEX'
insert into #wait_category select 'Other', 'XE_SESSION_CREATE_SYNC'
insert into #wait_category select 'Other', 'XE_SESSION_SYNC'
insert into #wait_category select 'Other', 'XE_STM_CREATE'
insert into #wait_category select 'Idle', 'XE_TIMER_EVENT'
insert into #wait_category select 'Other', 'XE_TIMER_MUTEX'
insert into #wait_category select 'Other', 'XE_TIMER_TASK_DONE'
insert into #wait_category select 'Idle', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
insert into #wait_category select 'Idle', 'HADR_WORK_QUEUE'
insert into #wait_category select 'Idle', 'HADR_LOGCAPTURE_WAIT'
insert into #wait_category select 'Idle', 'HADR_NOTIFICATION_DEQUEUE'
insert into #wait_category select 'Idle', 'HADR_CLUSAPI_CALL'
insert into #wait_category select 'Idle', 'HADR_TIMER_TASK'

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
            SELECT top (@TOP_N)
               'Wait types - Historical' as OUTPUT_TYPE
               , @BATCH_COUNTER AS BATCH_ID
               , coalesce(wc.category_name, 'Not Categorized') as category_name
               , c.wait_type
               , waiting_tasks_count
               , wait_time_ms
               , signal_wait_time_ms
               , CAST( case when waiting_tasks_count = 0 then 0 
                           else wait_time_ms *1.0/waiting_tasks_count end as decimal(38, 2)) as avg_ms_per_wait
               , signal_wait_time_ms
               , CAST( case when waiting_tasks_count = 0 then 0 
                           else signal_wait_time_ms *1.0/waiting_tasks_count end as decimal(38, 2)) as avg_signal_wait_time_ms_per_wait
               , max_wait_time_ms
            FROM #CURRENT c
            left join #wait_category wc on wc.wait_type = c.wait_type      
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
   , coalesce(wc.category_name, 'Not Categorized') as category_name
   , h.wait_type
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
   from #HISTORY h
   left join #wait_category wc on wc.wait_type = h.wait_type      
   where batch_id <> 1
   group by h.wait_type, coalesce(wc.category_name, 'Not Categorized')

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
    , coalesce(wc.category_name, 'Not Categorized') as category_name
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
left join #wait_category wc on wc.wait_type = h.wait_type      
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

