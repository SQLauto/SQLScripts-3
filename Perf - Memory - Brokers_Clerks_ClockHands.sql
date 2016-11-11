declare @group_by_node bit = 0
declare @show_clerks_with_zero_memory int = 0

select
 sum(pages_kb                    )  as pages_kb                    
, sum(virtual_memory_reserved_kb  ) as virtual_memory_reserved_kb  
, sum(virtual_memory_committed_kb ) as virtual_memory_committed_kb 
, sum(awe_allocated_kb            ) as awe_allocated_kb            
, sum(shared_memory_reserved_kb   ) as shared_memory_reserved_kb   
, sum(shared_memory_committed_kb  ) as shared_memory_committed_kb 
,  sum(pages_kb  ) + sum(virtual_memory_committed_kb )
 from sys.dm_os_memory_clerks
select committed_kb
, pages_plus_virtual_committed
, committed_kb - pages_plus_virtual_committed as missing_kb
, shared_memory_committed_kb
, awe_allocated_kb
from sys.dm_os_sys_info
cross join 	(select
 sum(pages_kb                    )  as pages_kb                    
, sum(virtual_memory_reserved_kb  ) as virtual_memory_reserved_kb  
, sum(virtual_memory_committed_kb ) as virtual_memory_committed_kb 
, sum(awe_allocated_kb            ) as awe_allocated_kb            
, sum(shared_memory_reserved_kb   ) as shared_memory_reserved_kb   
, sum(shared_memory_committed_kb  ) as shared_memory_committed_kb 
,  sum(pages_kb  ) + sum(virtual_memory_committed_kb ) as pages_plus_virtual_committed
 from sys.dm_os_memory_clerks) clerks

IF OBJECT_ID(N'TEMPDB..#clerks') IS NOT NULL DROP TABLE #clerks
 
CREATE TABLE #clerks
(
      type                        nvarchar(60)
    , name                        nvarchar(256)
    , memory_node_id              varchar(3)
    , clerk_count                 int
    , memory_used_kb              bigint
    , pages_kb                    bigint
    , virtual_memory_committed_kb bigint
    , shared_memory_committed_kb  bigint
    , awe_allocated_kb            bigint
    , virtual_memory_reserved_kb  bigint
    , shared_memory_reserved_kb   bigint
)
insert into #clerks
select type
, case when type = 'USERSTORE_TOKENPERM' and name like 'ACRUserStore%' then 'ACRUserStore_*'
       when type = 'USERSTORE_TOKENPERM' and name like 'SecCtxtACRUserStore%' then 'SecCtxtACRUserStore_*'
       else name end as name
, case when @group_by_node = 1 then cast(memory_node_id as varchar(2)) else 'All' end as memory_node_id
, count(1)                          as clerk_count
,   sum(pages_kb                    )
  + sum(virtual_memory_committed_kb )
  + sum(shared_memory_committed_kb  ) as memory_used_kb
, sum(pages_kb                    ) as pages_kb                    
, sum(virtual_memory_committed_kb ) as virtual_memory_committed_kb 
, sum(shared_memory_committed_kb  ) as shared_memory_committed_kb  
, sum(awe_allocated_kb            ) as awe_allocated_kb            
, sum(virtual_memory_reserved_kb  ) as virtual_memory_reserved_kb  
, sum(shared_memory_reserved_kb   ) as shared_memory_reserved_kb 
 from sys.dm_os_memory_clerks
group by type
, case when type = 'USERSTORE_TOKENPERM' and name like 'ACRUserStore%' then 'ACRUserStore_*'
       when type = 'USERSTORE_TOKENPERM' and name like 'SecCtxtACRUserStore%' then 'SecCtxtACRUserStore_*'
       else name end 
, case when @group_by_node = 1 then cast(memory_node_id as varchar(2)) else 'All' end

SELECT
      type
    , name
    , memory_node_id
    , clerk_count
    , memory_used_kb
    , pages_kb
    , virtual_memory_committed_kb
    , shared_memory_committed_kb
    , awe_allocated_kb
    , virtual_memory_reserved_kb
    , shared_memory_reserved_kb
FROM #clerks
where (@show_clerks_with_zero_memory = 0 and memory_used_kb > 0)
    or
      @show_clerks_with_zero_memory = 1
order by memory_used_kb desc, type, name
