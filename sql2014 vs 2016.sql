/* this script compares 2014 and 2016 for differences in 
      DMOs, 
      Catalog Metadata, 
      configurations, 
      XEvents

to do so, it creates linked servers, and drops them when done.
*/
----------------------------------------------------------------------------------------------------
/* Linked Server creation*/
----------------------------------------------------------------------------------------------------

declare @linked_server_name_sql2014 sysname = 'sql2014'
declare @datasource_sql2014         sysname = 'johnkauf-hp840\sql2014'
declare @linked_server_name_sql2016 sysname = 'sql2016'
declare @datasource_sql2016         sysname = 'johnkauf-hp840\sql2016'

declare @drop_linked_servers_YN     bit     = 0
----------------------------------------------------------------------------------------------------
/* create linked server to 2014 instance*/ 

   if exists (select * from sys.servers where name = @linked_server_name_sql2014)
         EXEC master.dbo.sp_dropserver @server=@linked_server_name_sql2014, @droplogins='droplogins'

   EXEC master.dbo.sp_addlinkedserver @server = @linked_server_name_sql2014, @provider=N'SQLNCLI11', @datasrc=@datasource_sql2014, @srvproduct=''
   EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=@linked_server_name_sql2014,@useself=TRUE,@locallogin=NULL,@rmtuser=null,@rmtpassword=null;
----------------------------------------------------------------------------------------------------
/* create linked server to 2016 instance*/ 
  
   if exists (select * from sys.servers where name = @linked_server_name_sql2016)
         EXEC master.dbo.sp_dropserver @server=@linked_server_name_sql2016, @droplogins='droplogins'

   EXEC master.dbo.sp_addlinkedserver @server = @linked_server_name_sql2016, @provider=N'SQLNCLI11', @datasrc=@datasource_sql2016, @srvproduct=''
   EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=@linked_server_name_sql2016,@useself=TRUE,@locallogin=NULL,@rmtuser=null,@rmtpassword=null;

----------------------------------------------------------------------------------------------------
/* DMOs and Catalog Metadata*/
----------------------------------------------------------------------------------------------------

   IF OBJECT_ID(N'TEMPDB..#sql2014') IS NOT NULL DROP TABLE #sql2014
   IF OBJECT_ID(N'TEMPDB..#sql2016') IS NOT NULL DROP TABLE #sql2016
   IF OBJECT_ID(N'TEMPDB..#s16_new_changed_objects') IS NOT NULL DROP TABLE #s16_new_changed_objects
   IF OBJECT_ID(N'TEMPDB..#diffs') IS NOT NULL DROP TABLE #diffs
   IF OBJECT_ID(N'TEMPDB..#columns_added') IS NOT NULL DROP TABLE #columns_added
   IF OBJECT_ID(N'TEMPDB..#new_objects') IS NOT NULL DROP TABLE #new_objects
 
   select o.type_desc, s.name as schema_name, o.name as object_name,  c.name as column_name
   , hashbytes('md2', o.type_desc + '|' + s.name + '|' + o.name) as object_hash
   into #sql2014
   from  sql2014.master.sys.all_objects o
   join  sql2014.master.sys.all_columns c on c.object_id = o.object_id
   join  sql2014.master.sys.schemas     s on s.schema_id = o.schema_id
   where o.type_desc not in ('internal_table', 'SYSTEM_TABLE', 'USER_TABLE', '')

   select o.type_desc, s.name as schema_name, o.name as object_name, c.name as column_name
   , hashbytes('md2', o.type_desc + '|' + s.name + '|' + o.name) as object_hash
   ,  case when o.name like 'dm%' then 'DMO' else 'Metadata' end as object_class
   into #sql2016
   from  sql2016.master.sys.all_objects o
   join  sql2016.master.sys.all_columns c on c.object_id = o.object_id
   join  sql2016.master.sys.schemas     s on s.schema_id = o.schema_id
   where o.type_desc not in ('internal_table', 'SYSTEM_TABLE', 'USER_TABLE', '')

   --select 'all 2016 objects' as output_type, * 
   --from #sql2016 s16
   --left join #sql2014 s14 on  s14.object_hash = s16.object_hash
   --                       and s14.column_name = s16.column_name
   --order by s16.schema_name, s16.type_desc, s16.object_name, s16.column_name

   /* if 2014 column is null, 2016 object is either new or has a change*/
   select distinct s16.schema_name, s16.object_name, s16.object_hash, s16.object_class
   into #s16_new_changed_objects 
   from #sql2016 s16
   left join #sql2014 s14 on  s14.object_hash = s16.object_hash
                          and s14.column_name = s16.column_name
   where s14.schema_name is null

   select  s16.object_class
         , s16.type_desc   as s16_type_desc   
         , s16.schema_name as s16_schema_name
         , s16.object_name as s16_object_name
         , s16.column_name as s16_column_name
         , s16.object_hash as s16_object_hash
         , s14.type_desc   as s14_type_desc   
         , s14.schema_name as s14_schema_name
         , s14.object_name as s14_object_name
         , s14.column_name as s14_column_name
         , s14.object_hash as s14_object_hash
   into #diffs
   from #s16_new_changed_objects      new  
   join #sql2016      s16 on s16.object_hash = new.object_hash
   left join #sql2014 s14 on s14.object_hash = s16.object_hash
                          and s14.column_name = s16.column_name
 
   CREATE TABLE #columns_added
   (     columns_added_id int identity(1, 1)
       , object_class     varchar(8)
       , s16_type_desc    nvarchar(60)
       , s16_schema_name  nvarchar(128)
       , s16_object_name  nvarchar(128)
       , s16_object_hash  varbinary(8000)
   )
   insert into #columns_added  
      select distinct  object_class
                     , s16_type_desc  
                     , s16_schema_name
                     , s16_object_name
                     , s16_object_hash
      from #diffs
      where s14_schema_name is not null

   select distinct  D.object_class
                  , D.s16_type_desc  
                  , D.s16_schema_name
                  , D.s16_object_name
   into #new_objects
   from #diffs d
   left join #columns_added ca on ca.s16_object_hash   = d.s16_object_hash
   where ca.S16_type_desc is null

   SELECT 'New DMO/Metadata' AS OUTPUT_TYPE
       , object_class        as object_class
       , s16_type_desc       as type_desc
       , s16_schema_name + '.' + s16_object_name   as object_name
   FROM #new_objects
   ORDER BY  OBJECT_NAME



   /* pivot new columns for existing objects*/
   IF OBJECT_ID(N'TEMPDB..#columns_to_pivot') IS NOT NULL DROP TABLE #columns_to_pivot

   select c.columns_added_id, s16_column_name
   into #columns_to_pivot
   from #diffs d
   join #columns_added c on c.s16_object_hash = d.s16_object_hash
   where d.s14_column_name is null
   order by columns_added_id

   declare @counter int = 1
   declare @max_counter int = (select max(columns_added_id) from #columns_added)
   declare @column_csv nvarchar(4000) = '' 

   IF OBJECT_ID(N'TEMPDB..#new_columns_pivoted') IS NOT NULL DROP TABLE #new_columns_pivoted

   CREATE TABLE #new_columns_pivoted
   (      columns_added_id int
        , column_csv  nvarchar(4000)
   )
  
   while @counter <= @max_counter
   begin
   select @column_csv = @column_csv + s16_column_name + ', '
   from #columns_to_pivot 
   where columns_added_id = @counter

   insert into #new_columns_pivoted select @counter, left(@column_csv, len(@column_csv) - 1)

   set @column_csv = ''
   set @counter = @counter + 1
   end

   SELECT 'New Columns'   AS OUTPUT_TYPE
       , object_class     as object_class
       , s16_type_desc    as type_desc
       , s16_schema_name + '.' + s16_object_name  as object_name
       , column_csv       as new_in_sql2016
   from #columns_added ca
   join #new_columns_pivoted ncp on ncp.columns_added_id = ca.columns_added_id
   order by object_class,  object_name

----------------------------------------------------------------------------------------------------
/* new configurations*/
----------------------------------------------------------------------------------------------------

   IF OBJECT_ID(N'TEMPDB..#configurations_2014') IS NOT NULL DROP TABLE #configurations_2014
   IF OBJECT_ID(N'TEMPDB..#configurations_2016') IS NOT NULL DROP TABLE #configurations_2016

   select * 
   into #configurations_2014
   from sql2014.master.sys.configurations

   select * 
   into #configurations_2016
   from sql2016.master.sys.configurations

   select 'New Configurations' as output_type, s16.*
   from #configurations_2016 s16
   left join #configurations_2014 s14 on s14.name = s16.name
   where s14.name is null

----------------------------------------------------------------------------------------------------
/* Extended Events*/
----------------------------------------------------------------------------------------------------
         --select distinct object_name 
         --from #sql2016 
         --where object_name like '%xe_%' and object_name not like '%exec%' and object_name not like '%index%'

/* collect data*/

   IF OBJECT_ID(N'TEMPDB..#xe_packages_2014') IS NOT NULL DROP TABLE #xe_packages_2014
   IF OBJECT_ID(N'TEMPDB..#xe_objects_2014')  IS NOT NULL DROP TABLE #xe_objects_2014
   IF OBJECT_ID(N'TEMPDB..#xe_columns_2014')  IS NOT NULL DROP TABLE #xe_columns_2014
   IF OBJECT_ID(N'TEMPDB..#xe_maps_2014')     IS NOT NULL DROP TABLE #xe_maps_2014

   IF OBJECT_ID(N'TEMPDB..#xe_packages_2016') IS NOT NULL DROP TABLE #xe_packages_2016
   IF OBJECT_ID(N'TEMPDB..#xe_objects_2016')  IS NOT NULL DROP TABLE #xe_objects_2016
   IF OBJECT_ID(N'TEMPDB..#xe_columns_2016')  IS NOT NULL DROP TABLE #xe_columns_2016
   IF OBJECT_ID(N'TEMPDB..#xe_maps_2016')     IS NOT NULL DROP TABLE #xe_maps_2016

   select distinct name, description -- 2 SQL Server packages?
   into #xe_packages_2014
   from sql2014.master.sys.dm_xe_packages

   select p.name as package_name
         , o.name   as object_name
         , o.object_type
         , o.description
         , o.capabilities_desc
         , o.type_name
         , o.type_size
   into #xe_objects_2014
   from sql2014.master.sys.dm_xe_objects o
   join sql2014.master.sys.dm_xe_packages p on p.guid = o.package_guid

   select p.name as package_name
         , c.object_name
         , c.name as column_name
         , c.column_id
         , c.description 
         , c.type_name
         , c.column_type
         , c.column_value
         , c.capabilities_desc
   into #xe_columns_2014
   from sql2014.master.sys.dm_xe_object_columns c
   join sql2014.master.sys.dm_xe_packages p on p.guid = c.object_package_guid

   select p.name as package_name
      , m.name as map_name
      , m.map_key
      , m.map_value
   into #xe_maps_2014
   from sql2014.master.sys.dm_xe_map_values m
   join sql2014.master.sys.dm_xe_packages p on p.guid = m.object_package_guid


   select distinct name, description -- 2 SQL Server packages?
   into #xe_packages_2016
   from sql2016.master.sys.dm_xe_packages

   select p.name as package_name
         , o.name   as object_name
         , o.object_type
         , o.description
         , o.capabilities_desc
         , o.type_name
         , o.type_size
   into #xe_objects_2016
   from sql2016.master.sys.dm_xe_objects o
   join sql2016.master.sys.dm_xe_packages p on p.guid = o.package_guid

   select p.name as package_name
         , c.object_name
         , c.name as column_name
         , c.column_id
         , c.description 
         , c.type_name
         , c.column_type
         , c.column_value
         , c.capabilities_desc
   into #xe_columns_2016
   from sql2016.master.sys.dm_xe_object_columns c
   join sql2016.master.sys.dm_xe_packages p on p.guid = c.object_package_guid

   select p.name as package_name
      , m.name as map_name
      , m.map_key
      , m.map_value
   into #xe_maps_2016
   from sql2016.master.sys.dm_xe_map_values m
   join sql2016.master.sys.dm_xe_packages p on p.guid = m.object_package_guid

/* new XE packages*/

   select 'New XE Packages' as Output_Type
   , s16.*
   from #xe_packages_2016 s16
   left join #xe_packages_2014 s14 on s14.name = s16.name
   where s14.name is null
   order by s16.name

/* new XE objects*/

   select 'New XE Objects' as Output_Type
   , s16.*
   from #xe_objects_2016 s16
   left join #xe_objects_2014 s14 on s14.package_name = s16.package_name
                                    and s14.object_name = s16.object_name
   where s14.object_name is null
   order by s16.package_name, object_name

/* new XE columms for existing objects*/

   select 'New XE Columns' as Output_Type
   , s16C.*
   from #xe_objects_2016 s16
   join #xe_objects_2014 s14 on s14.package_name = s16.package_name
                                    and s14.object_name = s16.object_name
   join #xe_columns_2016 s16c on s16c.package_name = s16.package_name
                                    and s16c.object_name = s16.object_name
   left join #xe_columns_2014 s14c on s14c.package_name = s14.package_name
                                    and s14c.object_name = s14.object_name
                                    and s14c.column_name = s16c.column_name
   where s14c.column_name is null
   order by s16C.package_name, S16C.object_name

/* New XE Maps*/
IF OBJECT_ID(N'TEMPDB..#new_maps') IS NOT NULL DROP TABLE #new_maps
IF OBJECT_ID(N'TEMPDB..#new_maps_with_values') IS NOT NULL DROP TABLE #new_maps_with_values

create table #new_maps_with_values (new_map_id int, values_csv nvarchar(4000))
 
select s16.* , row_number() over(order by s16.package_name, s16.map_name) new_map_id
into #new_maps
from      (select distinct package_name, map_name from #xe_maps_2016) s16
left join (select distinct package_name, map_name from #xe_maps_2014) s14 
on s16.package_name = s14.package_name
and s16.map_name = s14.map_name
where s14.map_name is null

declare @values_csv nvarchar(4000) = ''
declare @map_counter int = 1
declare @map_max_counter int = (select count(1) from #new_maps)

while @map_counter <= @map_max_counter
   begin
      select @values_csv = @values_csv + map_value + ',   '
      from #new_maps nm
      join #xe_maps_2016 s16 on s16.map_name = nm.map_name and s16.package_name = nm.package_name
      where nm.new_map_id = @map_counter

      insert into #new_maps_with_values select @map_counter, left(@values_csv, len(@values_csv) - 1)

      set @values_csv = ''
      set @map_counter = @map_counter + 1
   end

select 'New XE Maps' as Output_Type
, package_name
, map_name
, values_csv
from #new_maps nm
join #new_maps_with_values v on v.new_map_id = nm.new_map_id
order by package_name, map_name

----------------------------------------------------------------------------------------------------
/* Drop linked servers when done*/ 
----------------------------------------------------------------------------------------------------
if @drop_linked_servers_YN = 1
begin
   if exists (select * from sys.servers where name = @linked_server_name_sql2014)
         EXEC master.dbo.sp_dropserver @server=@linked_server_name_sql2014, @droplogins='droplogins'

   if exists (select * from sys.servers where name = @linked_server_name_sql2016)
         EXEC master.dbo.sp_dropserver @server=@linked_server_name_sql2016, @droplogins='droplogins'
end