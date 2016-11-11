/* separate entries for each resource pool.
   resource_semaphore_id = 0:  large queries
                         = 1:  small queries
*/
SELECT * FROM sys.dm_exec_query_resource_semaphores

--/* get largest memory grant recorded by resource pool */

--select * from sys.dm_resource_governor_resource_pools

select name, max_request_grant_memory_kb 
from sys.dm_resource_governor_workload_groups
where name <> 'internal'

/* loop over memory grants at session level */

declare @loop_count            int = 2  -- loops not likely needed if persisting tables and running via sql job.
declare @loop_interval_seconds int = 1

IF OBJECT_ID(N'TEMPDB..#memory_grants') IS NOT NULL DROP TABLE #memory_grants
 
CREATE TABLE #memory_grants
(
    batch_date              date
    , batch_datetime        datetime
    , database_name         nvarchar(128)
    , login_name            nvarchar(128)
    , sql_text              nvarchar(MAX)
    , session_id            smallint
    , request_id            int
    , scheduler_id          int
    , dop                   smallint
    , request_time          datetime
    , grant_time            datetime
    , requested_memory_kb   bigint
    , granted_memory_kb     bigint
    , required_memory_kb    bigint
    , used_memory_kb        bigint
    , max_used_memory_kb    bigint
    , query_cost            float(53)
    , timeout_sec           int
    , resource_semaphore_id smallint
    , queue_id              smallint
    , wait_order            int
    , is_next_candidate     bit
    , wait_time_ms          bigint
    , plan_handle           varbinary(64)
    , sql_handle            varbinary(64)
    , group_id              int
    , pool_id               int
    , is_small              bit
    , ideal_memory_kb       bigint
    --, reserved_worker_count int
    --, used_worker_count     int
    --, max_used_worker_count int
    --, reserved_node_bitmap  bigint

)



while @loop_count > 0
begin

declare @now datetime = getdate()
declare @date date = cast(@now as date)

   insert into #memory_grants
      SELECT  @date
      ,@now
      , DB_NAME(ST.DBID) AS [DATABASENAME]
      , s.login_name
      , ST.[TEXT]
      , mg.*
      FROM SYS.DM_EXEC_QUERY_MEMORY_GRANTS AS MG
      join sys.dm_exec_sessions            as s on s.session_id = MG.session_id
      CROSS APPLY SYS.DM_EXEC_SQL_TEXT(PLAN_HANDLE) AS ST

      waitfor delay @loop_interval_seconds
set @loop_count = @loop_count - 1

end

  
SELECT
      batch_date
    , batch_datetime
    , database_name
    , login_name
    , session_id
    , request_id
    , scheduler_id
    , dop
    , request_time
    , grant_time
    , requested_memory_kb
    , granted_memory_kb
    , required_memory_kb
    , used_memory_kb
    , max_used_memory_kb
    , query_cost
    , timeout_sec
    , resource_semaphore_id
    , queue_id
    , wait_order
    , is_next_candidate
    , wait_time_ms
    , plan_handle
    , sql_handle
    , group_id
    , pool_id
    , is_small
    , ideal_memory_kb
    , left(replace(replace(replace(sql_text, char(10), ' '), char(13), ' '), char(9), ' '), 100) as sql_text
FROM #memory_grants
-- where granted_memory_kb is null or granted_memory_kb = 0 or requested_memory_kb > requested_memory_kb
