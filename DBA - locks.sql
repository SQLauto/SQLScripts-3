

while 1 = 1
begin
select request_session_id, resource_type, resource_database_id, resource_description, request_mode, request_status
from sys.dm_tran_locks
end

while 1 = 1
begin
select request_session_id, resource_type, resource_database_id, request_mode, request_status, count(1)
from sys.dm_tran_locks
group by request_session_id, resource_type, resource_database_id, request_mode, request_status
order by request_session_id, resource_type, resource_database_id, request_mode, request_status
end