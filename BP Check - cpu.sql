/* hack of bp check script + extra stuff to pull cpu-related items.

includes 
   -- configurations (power plan; cpu count; numa nodes; affinity masking; max dop; forced parameterizations)
   -- cpu utilization from system health xevent
   -- sys.dm_os_schedulers
   -- wait stats analysis
   -- high cpu queries
*/

--------------------------------------------------------------------------------------------------------------------------------
-- Power plan subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Power plan', 10, 1) WITH NOWAIT
DECLARE @planguid NVARCHAR(64), @powerkey NVARCHAR(255), @winver VARCHAR(5) 
--SELECT @powerkey = 'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\ControlPanel\NameSpace\{025A5937-A6BE-4686-A844-36FE4BEC8B6D}'
--SELECT @powerkey = 'SYSTEM\CurrentControlSet\Control\Power\User\Default\PowerSchemes'
SELECT @powerkey = 'SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes'

IF @winver >= 6.0
BEGIN
	BEGIN TRY
		--EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @powerkey, 'PreferredPlan', @planguid OUTPUT, NO_OUTPUT
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @powerkey, 'ActivePowerScheme', @planguid OUTPUT, NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Power plan subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END

-- http://support.microsoft.com/kb/935799/en-us

IF @winver IS NULL 
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[WARNING: Could not determine Windows version for check]' AS [Deviation]
END
ELSE IF @planguid IS NOT NULL AND @planguid <> '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[WARNING: The current power plan scheme is not recommended for database servers. Please reconfigure for High Performance mode]' AS [Deviation]
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Information], CASE WHEN @planguid = '381b4222-f694-41f0-9685-ff5bb260df2e' THEN 'Balanced'
		WHEN @planguid = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c' THEN 'High Performance'
		WHEN @planguid = 'a1841308-3541-4fab-bc81-f71556f20b4a' THEN 'Power Saver'
		ELSE 'Other' END AS [Power_Plan]
END
ELSE IF @planguid IS NOT NULL AND @planguid = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Number of available Processors for this instance vs. MaxDOP setting subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Number of available Processors for this instance vs. MaxDOP setting', 10, 1) WITH NOWAIT
DECLARE @cpucount int, @numa int, @affined_cpus int
, @cpuaffin VARCHAR(255)
SELECT @cpucount = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64
SELECT @numa = COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64;

;WITH bits AS 
(SELECT 7 AS N, 128 AS E UNION ALL SELECT 6, 64 UNION ALL 
SELECT 5, 32 UNION ALL SELECT 4, 16 UNION ALL SELECT 3, 8 UNION ALL 
SELECT 2, 4 UNION ALL SELECT 1, 2 UNION ALL SELECT 0, 1), 
bytes AS 
(SELECT 1 M UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)
-- CPU Affinity is shown highest to lowest CPU ID
SELECT @cpuaffin = CASE WHEN [value] = 0 THEN REPLICATE('1', @cpucount)
	ELSE RIGHT((SELECT ((CONVERT(tinyint, SUBSTRING(CONVERT(binary(9), [value]), M, 1)) & E) / E) AS [text()] 
		FROM bits CROSS JOIN bytes
		ORDER BY M, N DESC
		FOR XML PATH('')), @cpucount) END
FROM sys.configurations (NOLOCK)
WHERE name = 'affinity mask';

DECLARE @i int, @cpuaffin_fixed VARCHAR(300)
SET @cpuaffin_fixed = @cpuaffin
SET @i = @cpucount/@numa + 1
WHILE @i <= @cpucount
BEGIN
	SELECT @cpuaffin_fixed = STUFF(@cpuaffin_fixed, @i, 1, '_' + SUBSTRING(@cpuaffin, @i, 1))
	SET @i = @i + @cpucount/@numa + 1
END


SELECT @affined_cpus = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64;
--SELECT @cpucount = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64
SELECT 'Processor_checks' AS [Category], 'Parallelism_MaxDOP' AS [Check],
	CASE WHEN [value] > @affined_cpus THEN '[WARNING: MaxDOP setting exceeds available processor count (affinity)]'
		WHEN @numa = 1 AND @affined_cpus > 8 AND ([value] = 0 OR [value] > 8) THEN '[WARNING: MaxDOP setting is not recommended for current processor count (affinity)]'
		WHEN @numa > 1 AND (@cpucount/@numa) < 8 AND ([value] = 0 OR [value] > (@cpucount/@numa)) THEN '[WARNING: MaxDOP setting is not recommended for current NUMA node to processor count (affinity) ratio]'
		WHEN @numa > 1 AND (@cpucount/@numa) >= 8 AND ([value] = 0 OR [value] > 8 OR [value] > (@cpucount/@numa)) THEN '[WARNING: MaxDOP setting is not recommended for current NUMA node to processor count (affinity) ratio]'
		ELSE '[OK]'
	END AS [Deviation]
FROM sys.configurations (NOLOCK) WHERE name = 'max degree of parallelism';

SELECT 'Processor_checks' AS [Category], 'Parallelism_MaxDOP' AS [Information], 
	CASE WHEN [value] > @affined_cpus THEN @affined_cpus
		WHEN @numa = 1 AND @affined_cpus > 8 AND ([value] = 0 OR [value] > 8) THEN 8
		WHEN @numa > 1 AND (@cpucount/@numa) < 8 AND ([value] = 0 OR [value] > (@cpucount/@numa)) THEN @cpucount/@numa
		WHEN @numa > 1 AND (@cpucount/@numa) >= 8 AND ([value] = 0 OR [value] > 8 OR [value] > (@cpucount/@numa)) THEN 8
		ELSE 0
	END AS [Recommended_MaxDOP],
	[value] AS [Current_MaxDOP], @cpucount AS [Available_Processors], @affined_cpus AS [Affined_Processors], 
	-- Processor Affinity is shown highest to lowest CPU ID
	@cpuaffin_fixed AS Affinity_Mask_Bitmask
FROM sys.configurations (NOLOCK) WHERE name = 'max degree of parallelism';

--------------------------------------------------------------------------------------------------------------------------------
-- Processor Affinity in NUMA architecture subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Processor Affinity in NUMA architecture', 10, 1) WITH NOWAIT
IF @numa > 1
BEGIN
	WITH ncpuCTE (ncpus) AS (SELECT COUNT(cpu_id) AS ncpus from sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64 GROUP BY parent_node_id, is_online HAVING COUNT(cpu_id) = 1),
	cpuCTE (node, afin) AS (SELECT DISTINCT(parent_node_id), is_online FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64 GROUP BY parent_node_id, is_online)
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Check],
		CASE WHEN (SELECT COUNT(*) FROM ncpuCTE) > 0 THEN '[WARNING: Current NUMA configuration is not recommended. At least one node has a single assigned CPU]' 
			WHEN (SELECT COUNT(DISTINCT(node)) FROM cpuCTE WHERE afin = 0 AND node NOT IN (SELECT DISTINCT(node) FROM cpuCTE WHERE afin = 1)) > 0 THEN '[WARNING: Current NUMA configuration is not recommended. At least one node does not have assigned CPUs]' 
			ELSE '[OK]' END AS [Deviation]
	FROM sys.dm_os_sys_info (NOLOCK) 
	OPTION (RECOMPILE);
	
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Information], cpu_count AS [Logical_CPU_Count], 
		(SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64) AS [NUMA_Nodes],
		-- Processor Affinity is shown highest to lowest CPU ID
		@cpuaffin_fixed AS Affinity_Mask_Bitmask
	FROM sys.dm_os_sys_info (NOLOCK) 
	OPTION (RECOMPILE);
END
ELSE
BEGIN
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Check], '[Not_NUMA]' AS [Deviation]
	FROM sys.dm_os_sys_info (NOLOCK)
	OPTION (RECOMPILE);
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Additional Processor information subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Additional Processor information', 10, 1) WITH NOWAIT

-- Processor Info
SELECT 'Processor_checks' AS [Category], 'Processor_Summary' AS [Information], cpu_count AS [Logical_CPU_Count], hyperthread_ratio AS [Cores2Socket_Ratio],
	cpu_count/hyperthread_ratio AS [CPU_Sockets], 
	CASE WHEN @numa > 1 THEN (SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64) ELSE 0 END AS [NUMA_Nodes],
	@affined_cpus AS [Affined_Processors], 
	-- Processor Affinity is shown highest to lowest Processor ID
	@cpuaffin_fixed AS Affinity_Mask_Bitmask
FROM sys.dm_os_sys_info (NOLOCK)
OPTION (RECOMPILE);

BEGIN
	RAISERROR (N'  |-Starting Processor utilization rate in the last 2 hours', 10, 1) WITH NOWAIT
	-- Processor utilization rate in the last 2 hours
	DECLARE @ts_now bigint
	DECLARE @tblAggCPU TABLE (SQLProc int, SysIdle int, OtherProc int, Minutes int)
	SELECT @ts_now = ms_ticks FROM sys.dm_os_sys_info (NOLOCK);

	WITH cteCPU (record_id, SystemIdle, SQLProcessUtilization, [timestamp]) AS (SELECT 
			record.value('(./Record/@id)[1]', 'int') AS record_id,
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
			[TIMESTAMP] FROM (SELECT [TIMESTAMP], CONVERT(xml, record) AS record 
				FROM sys.dm_os_ring_buffers (NOLOCK)
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
				AND record LIKE '%<SystemHealth>%') AS x
		)
	INSERT INTO @tblAggCPU
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 10 
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -10, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 20
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -10, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -20, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 30
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -20, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -30, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 40
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -30, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -40, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 50
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -40, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -50, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 60
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -50, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -60, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 70
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -60, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -70, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 80
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -70, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -80, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 90
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -80, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -90, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 100
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -90, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -100, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 110
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -100, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -110, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 120
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -110, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -120, GETDATE())
	
	IF (SELECT COUNT(SysIdle) FROM @tblAggCPU WHERE SysIdle < 30) > 0
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[WARNING: Detected CPU usage over 70 pct]' AS [Deviation];
	END
	ELSE IF (SELECT COUNT(SysIdle) FROM @tblAggCPU WHERE SysIdle < 10) > 0
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[WARNING: Detected CPU usage over 90 pct]' AS [Deviation];
	END
	ELSE
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[OK]' AS [Deviation];
	END;

	SELECT 'Processor_checks' AS [Category], 'Agg_Processor_Usage_last_2h' AS [Information], SQLProc AS [SQL_Process_Utilization], SysIdle AS [System_Idle], OtherProc AS [Other_Process_Utilization], Minutes AS [Time_Slice_min]
	FROM @tblAggCPU;
END;

RAISERROR (N'|-Starting Memory Checks', 10, 1) WITH NOWAIT

