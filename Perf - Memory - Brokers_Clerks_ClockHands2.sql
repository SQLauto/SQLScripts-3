/* 


To get a persective on SQL memory usage within the context of the OS, use Perf - Memory - Server+Node+Process+MemMgr.sql


* 
Script:         this script returns information about SQL's internal memory usage.

Purpose:        returns information about brokers and clerks, looping to capture metrics over time.
                clock hand information is also captured.

Functionality:  User can set the number of loops and loop interval for data collection.
                

Source:
               sys.dm_os_memory_brokers
               sys.dm_os_memory_clerks
               sys.dm_os_memory_cache_clock_hands

Usage           Query requires no persisted tables to use.  Output sent to UI by default.  

Author          John Kauffman

Version         2016-09-12 1.0 - Initial version

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

DECLARE @LOOP_COUNT            INT = 3  -- LOOPS NOT LIKELY NEEDED IF PERSISTING TABLES AND RUNNING VIA SQL JOB
DECLARE @LOOP_INTERVAL_SECONDS INT = 1

DECLARE @FILTER_ZERO_VALUES    BIT = 1

---------------------------------------------------------------------------------------------
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



IF OBJECT_ID(N'TEMPDB..#memory_brokers') IS NOT NULL DROP TABLE #memory_brokers
 
CREATE TABLE #memory_brokers
(     batch_datetime           datetime  
    , batch_number             int
    , pool_id                  int
    , memory_broker_type       nvarchar(60)
    , allocations_kb           bigint
    , allocations_kb_per_sec   bigint
    , predicted_allocations_kb bigint
    , target_allocations_kb    bigint
    , future_allocations_kb    bigint
    , overall_limit_kb         bigint
    , last_notification        nvarchar(60)
)

IF OBJECT_ID(N'TEMPDB..#memory_clerks') IS NOT NULL DROP TABLE #memory_clerks
 
CREATE TABLE #memory_clerks
(     batch_datetime              datetime 
    , batch_number             int 
    , memory_clerk_address        varbinary(8)
    , type                        nvarchar(60)
    , name                        nvarchar(256)
    , memory_node_id              smallint
    , pages_kb                    bigint
    , virtual_memory_reserved_kb  bigint
    , virtual_memory_committed_kb bigint
    , awe_allocated_kb            bigint
    , shared_memory_reserved_kb   bigint
    , shared_memory_committed_kb  bigint
)
  

 IF OBJECT_ID(N'TEMPDB..#cache_clock_hands') IS NOT NULL DROP TABLE #cache_clock_hands
 
CREATE TABLE #cache_clock_hands
(     batch_datetime           datetime 
    , batch_number             int 
    , cache_address            varbinary(8)
    , name                     nvarchar(256)
    , type                     nvarchar(60)
    , clock_hand               nvarchar(60)
    , clock_status             nvarchar(60)
    , rounds_count             bigint
    , removed_all_rounds_count bigint
    , updated_last_round_count bigint
    , removed_last_round_count bigint
    , last_tick_time           bigint
    , round_start_time         bigint
    , last_round_start_time    bigint
) 

raiserror('|- end   prep work', 10, 1) with nowait
raiserror('|--------------------------------------------------------------', 10, 1) with nowait

raiserror('|- begin loop', 10, 1) with nowait

while @loop_count >= 1
   begin

      set @now = getdate()
      set @batch_date = cast(@now as date)

      insert into #memory_brokers
         select @now   
               , @batch_counter   
               , pool_id                  
               , memory_broker_type       
               , allocations_kb           
               , allocations_kb_per_sec   
               , predicted_allocations_kb 
               , target_allocations_kb    
               , future_allocations_kb    
               , overall_limit_kb         
               , last_notification        
         from sys.dm_os_memory_brokers
         WHERE ((@FILTER_ZERO_VALUES = 1 AND ALLOCATIONS_KB > 0)
                 OR 
                 @FILTER_ZERO_VALUES = 0)

      insert into #memory_clerks
         select @now  
               , @batch_counter       
               , memory_clerk_address        
               , type                        
               , name                        
               , memory_node_id              
               , pages_kb                    
               , virtual_memory_reserved_kb  
               , virtual_memory_committed_kb 
               , awe_allocated_kb            
               , shared_memory_reserved_kb   
               , shared_memory_committed_kb  
         from sys.dm_os_memory_clerks
         WHERE ((@FILTER_ZERO_VALUES = 1 AND pages_kb + virtual_memory_committed_kb + awe_allocated_kb > 0)
                 OR 
                 @FILTER_ZERO_VALUES = 0)

/* NOT FILTERING INSERTS HERE, SINCE I NEED TO COMPARE TWO POINTS IN TIME (CLOCK HANDS ACCUMULATE SINCE SERVER RESTART).
   BY NOT FILTERING, I MAKE THE JOINS EASIER.*/
      insert into #cache_clock_hands 
         select @now  
             , @batch_counter       
             , cache_address            
             , name                     
             , type                     
             , clock_hand               
             , clock_status             
             , rounds_count             
             , removed_all_rounds_count 
             , updated_last_round_count 
             , removed_last_round_count 
             , last_tick_time           
             , round_start_time         
             , last_round_start_time    
         from sys.dm_os_memory_cache_clock_hands

      raiserror('   |- loop %d of %d complete', 10, 1, @batch_counter, @original_loop_value) with nowait

      set @LOOP_COUNT = @LOOP_COUNT - 1
      set @batch_counter = @batch_counter + 1

   end -- while @loop_count >= 1

raiserror('|- end   loop', 10, 1) with nowait
raiserror('|--------------------------------------------------------------', 10, 1) with nowait
----------------------------------------------------------------------------------------------------

raiserror('|- begin output', 10, 1) with nowait

SELECT * FROM #memory_brokers
ORDER BY BATCH_NUMBER, ALLOCATIONS_KB DESC

SELECT *
FROM #memory_clerks
ORDER BY BATCH_NUMBER, pages_kb + virtual_memory_committed_kb DESC

SELECT
      C2.batch_datetime
    , C2.batch_number
    , C2.name
    , C2.type
    , C2.clock_hand
    , C2.clock_status
    , C2.rounds_count - C1.rounds_count as rounds_count
    , C2.removed_all_rounds_count  - c1.removed_all_rounds_count   as removed_all_rounds_count 
    , C2.updated_last_round_count  - c1.updated_last_round_count   as updated_last_round_count
    , C2.removed_last_round_count  - c1.removed_last_round_count   as removed_last_round_count
    , C2.last_tick_time            
    , C2.round_start_time          
    , C2.last_round_start_time
FROM #cache_clock_hands C1
JOIN #cache_clock_hands C2 ON C1.BATCH_NUMBER = C2.BATCH_NUMBER - 1
AND C1.CLOCK_HAND = C2.clock_hand
AND C1.NAME = C2.NAME
AND C1.TYPE = C2.TYPE
ORDER BY C2.BATCH_NUMBER
