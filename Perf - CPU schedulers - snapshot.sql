declare @show_visible_schedulers bit = 1
declare @show_hidden_schedulers  bit = 1
declare @show_DAC                bit = 0
---------------------------------------------------
declare @show_offline_schedulers bit = 0


/* what are the schedulers doing?.

Note that schedulers used for user activity (visible online) are also managing 
internal processes.
note that current tasks for a scheduler_id doesn't match the total number of requests, though 
it should be close (timing issue one factor affecting)
*/

select 'requests on each scheduler' as output_type
   , s.parent_node_id
   , s.scheduler_id
   , s.cpu_id
   , s.status
   , case when r.session_id is not null then r.status
          else 'no current request' end as request_type
   , r.session_id
   , r.command
   , s.is_online
   , s.is_idle
   , s.yield_count            -- total times yielded, even if to itself
   , s.context_switches_count -- total times yielded to other schedulers
   -------------------------
   , s.idle_switches_count       -- number of times scheduler's been idle
   , cast(s.idle_switches_count * 1.0/(s.yield_count + s.idle_switches_count) * 100 as decimal(38, 2)) as idle_switches_pct_of_yield_and_idle

   , s.preemptive_switches_count -- times control passed to the OS (e.g., xp_cmdshell, clr, distributed queries)
   , cast(s.preemptive_switches_count * 1.0/(s.yield_count) * 100 as decimal(38, 2)) as preemptive_switches_pct_of_yield
   , s.load_factor              -- high load factor
   , s.current_tasks_count      -- high current tasks (all tasks in suspended, runnable, and running)
   , s.runnable_tasks_count     -- high values indicate a deep queue waiting to get on a scheduler.
   , s.work_queue_count
   , s.pending_disk_io_count
from sys.dm_os_schedulers s
left join sys.dm_exec_requests r on r.scheduler_id = s.scheduler_id
where (   (s.status like 'visible%' and s.status not like '%DAC%' and @show_visible_schedulers = 1)
       or (s.status like 'hidden%'  and @show_hidden_schedulers = 1)
       or (s.status like '%DAC%'    and @show_DAC = 1)
      )
   and (s.is_online = 1
      or s.is_online = 0 and @show_offline_schedulers = 1)
order by scheduler_id, session_id



/* indicators of historical load.
These use the cumulative count metrics.  see next query for snapshot values.

what percentage of switches are idle?
   indicates times of no work to be done. denominator = yield + idle.  
what's context switches vs yield?  
   (1 - context %) indicates number of times that the quanta was used up, but nothing else in the queue so the task was able to go right back to work.
what percentage of switches are preemptive (preemptive / total yield)?  
   changes from baseline may help pinpoint source of CPU increase as coming from increased external calls to OS.
 */


select 'historical patterns' as output_type
, s.parent_node_id
, s.scheduler_id
, s.cpu_id
, s.status
, s.is_online
, s.is_idle
, s.yield_count            -- total times yielded, even if to itself
-------------------------
, s.context_switches_count -- total times yielded to other schedulers
, cast(s.context_switches_count * 1.0/(s.yield_count) * 100 as decimal(38, 2)) as context_switches_pct_of_yield
-------------------------

, s.idle_switches_count       -- number of times scheduler's been idle
, cast(s.idle_switches_count * 1.0/(s.yield_count) * 100 as decimal(38, 2)) as idle_switches_pct_of_yield
-------------------------
, s.yield_count - s.context_switches_count - s.idle_switches_count as self_yielding_switches
, cast((s.yield_count - s.context_switches_count - s.idle_switches_count) * 1.0/(s.yield_count) * 100 as decimal(38, 2)) as self_yielding_pct_of_yield
-------------------------
, s.preemptive_switches_count -- times control passed to the OS (e.g., xp_cmdshell, clr, distributed queries)
, cast(s.preemptive_switches_count * 1.0/(s.yield_count+s.preemptive_switches_count) * 100 as decimal(38, 2)) as preemptive_switches_pct_of_yield_and_preemptive
from sys.dm_os_schedulers s
where (   (s.status like 'visible%' and s.status not like '%DAC%' and @show_visible_schedulers = 1)
       or (s.status like 'hidden%'  and @show_hidden_schedulers = 1)
       or (s.status like '%DAC%'    and @show_DAC = 1)
      )
   and (s.is_online = 1
      or s.is_online = 0 and @show_offline_schedulers = 1)

/* signs of current pressure.
The counters reflect current activity, rather than cumulative metrics.
So, they really need to be evaluated within context of a baseline.   

*/

select 'current pressure' as output_type
, parent_node_id
, scheduler_id
, cpu_id
, status
, is_online
, is_idle              -- no idle schedulers?
, load_factor          -- high load factor?
, current_tasks_count  -- high current tasks (all tasks in suspended, runnable, and running)
, runnable_tasks_count -- high values indicate a deep queue waiting to get on a scheduler.
, work_queue_count     -- tasks in pending queue.  not sure of difference from runnable tasks  
, pending_disk_io_count
from sys.dm_os_schedulers s
where (   (s.status like 'visible%' and s.status not like '%DAC%' and @show_visible_schedulers = 1)
       or (s.status like 'hidden%'  and @show_hidden_schedulers = 1)
       or (s.status like '%DAC%'    and @show_DAC = 1)
      )
   and (s.is_online = 1
      or s.is_online = 0 and @show_offline_schedulers = 1)
order by scheduler_id, parent_node_id

--yield_count - Internal value that is used to indicate progress on this scheduler. 
--      This value is used by the Scheduler Monitor to determine whether a worker on the scheduler is not yielding to 
--      other workers on time. This value does not indicate that the worker or task transitioned to a new worker. Is not nullable.

--context_switches_count - Number of context switches that have occurred on this scheduler. Is not nullable.
--      To allow for other workers to run, the current running worker has to 
--      relinquish control of the scheduler or switch context.
--      Note: If a worker yields the scheduler and puts itself into the runnable queue and then finds no other workers, 
--      the worker will select itself. In this case, the context_switches_count is not updated, but the yield_count is updated. 
 

--idle_switches_count - Number of times the scheduler has been waiting for an event while idle.
--      This column is similar to context_switches_count. Is not nullable.
 
--preemptive_switches_count - Number of times that workers on this scheduler have switched to the preemptive mode. 
--      To execute code that is outside SQL Server (for example, extended stored procedures and distributed queries), 
--      a thread has to execute outside the control of the non-preemptive scheduler. To do this, a worker switches to preemptive mode. 
 


--current_tasks_count - Number of current tasks that are associated with this scheduler. This count includes the following:
--      •Tasks that are waiting for a worker to execute them.
--      •Tasks that are currently waiting or running (in SUSPENDED or RUNNABLE state).
--.
--runnable_tasks_count - Number of workers, with tasks assigned to them, that are waiting to be scheduled on the runnable queue. 
--work_queue_count     - Number of tasks in the pending queue. These tasks are waiting for a worker to pick them up. Is not nullable.
 

--current_workers_count - Number of workers that are associated with this scheduler. 
--      This count includes workers that are not assigned any task. Is not nullable.
 

--active_workers_count - Number of workers that are active. 
--      An active worker is never preemptive, must have an associated task, and is either running, runnable, or suspended. Is not nullable.
 

 

--pending_disk_io_count - Number of pending I/Os that are waiting to be completed. Each scheduler has a list 
--      of pending I/Os that are checked to determine whether they have been completed every time there is a context switch. 
--      The count is incremented when the request is inserted. This count is decremented when the request is completed. 
--      This number does not indicate the state of the I/Os. Is not nullable.
 

--load_factor - Internal value that indicates the perceived load on this scheduler. This value is used to determine 
--      whether a new task should be put on this scheduler or another scheduler. This value is useful for debugging 
--      purposes when it appears that schedulers are not evenly loaded. The routing decision is made based on the 
--      load on the scheduler. SQL Server also uses a load factor of nodes and schedulers to help determine 
--      the best location to acquire resources. When a task is enqueued, the load factor is increased. 
--      When a task is completed, the load factor is decreased. Using the load factors helps SQL Server OS balance 
--      the work load better. Is not nullable.
 

 
