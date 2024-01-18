--TODO: CREATE script for COLUMNSTORE indexes (clustered and nonclustered). Add filegroup/partition_scheme to index CREATE script.
-- Download current version at: https://blog.sqlxdetails.com/sp_xdetails/
USE master -- must be in master. Otherwise proc is not found and is not in context of current db.
GO
PRINT 'INSTRUCTIONS for SQL Management Studio:
	Tools -> Options -> Keyboard -> Query Shortcuts
		Next to "CTRL+F1" enter "sp_xdetails" (without quotes, no "dbo.")
		If you are on Mac, or CTRL+F1 does not work for some reason, try assigning to "CTRL+3".
	Tools -> Options -> Query Results -> SQL Server -> Results to Grid
		"Retain CR/LF on copy or save" - tick
		"Non XML data" - change from 65535 to 1065535
	Close all Query Windows (or entire SSMS). Settings work only for newly opened "Query Windows".
';
IF OBJECT_ID('dbo.sp_xdetails') IS NULL EXEC('CREATE PROC dbo.sp_xdetails AS');
GO
ALTER PROCEDURE dbo.sp_xdetails
-- v2.63 written by Vedran Kesegic
-- INSTRUCTIONS for SQL Management Studio:
--		Tools -> Options -> Keyboard -> Query Shortcuts
--			Next to "CTRL+F1" enter "sp_xdetails" (without quotes, no "dbo.")
--		Tools -> Options -> Query Results -> SQL Server -> Results to Grid
--			"Retain CR/LF on copy or save" - tick
--			"Non XML data" - change from 65535 to 1065535
--		Close all Query Windows (or entire SSMS). Settings work only for newly opened "Query Windows".
(	@search NVARCHAR(300) -- name, schema.name, part of name (% allowed) = search. To get extended info, call twice within 3 seconds!
) AS
-- Author: Vedran Kesegic
-- 2019-07-03 Initial version (from twin brother: SQL xDetails plugin for SQL Management Studio)
-- 2019-07-27 Added search, triggers, and @Extended parameter to hide less needed outputs like total size.
-- 2019-07-30 Added check constraints, foreign keys (referenced tables).
-- 2019-08-24 Added index usage in plans and actual index usage, as extended info. Added source code for proc/fun/trig.
-- 2019-08-24 Bugfix: "schema.object" did not work, fails when sp_xdetails has >1 parameter. Therefore @Extended is removed and can be obtained with "+" sign as first character.
-- 2019-09-01 Added ability to show sub-objects by name, eg. triggers
-- 2019-10-05 Better format of some numbers (3 digit grouping). Extended mode you get with double-call.
-- 2019-10-26 Added index type info
-- 2019-11-03 Isolation level and other settings on the top, making proc more reliable and faster in some setting combinations
-- 2019-11-03 Added parameters info
-- 2019-11-20 Added synonym info. Added is_ms_shipped to search.
-- 2020-01-16 Added object name and type info in messages. Added "Has Index" to foreign keys.
-- 2020-02-22 Added CREATE INDEX script to indexes
-- 2020-02-26 Turned off io and time stats for this proc, not to mess the output. Increased "extended" double-call to 5 sec.
-- 2020-03-10 Added compression info, improved scripting to include compression, ignore dup key, row/page locking, pad index
-- 2020-07-09 Added key columns into index usage extended information
-- 2020-08-19 Added sequence info. Improved search.
-- 2020-10-04 Bugfix when searching object name in brackets. Added message when object is not found.
-- 2020-11-25 Added Statistics info in "Extended" part
-- 2020-12-14 Bugfix: throwing errors for TVF in detailed mode
-- 2021-02-04 Added partitions in detailed mode
-- 2021-02-11 Added fragmentation info for partitions in detailed mode
-- 2021-02-12 Added columnstore segment info in detailed mode. Kudos Domagoj Peharda for the fix!
-- 2021-05-11 Including heaps in list of indexes.
-- 2021-05-23 Added more details about statistics
-- 2021-06-03 Bugfix: now it works in Azure
-- 2021-06-11 Added trigger definition in the list of triggers
-- 2021-09-19 Added data space (filegroup) info to index details
-- 2021-10-09 Index create script: added columnstore support and fixed unique constraint scripting (was scripted as unique index before)
-- 2021-11-29 Added object definition to search
-- 2022-10-19 Minor fix (CONCAT_NULL_YIELDS_NULL error)
-- 2023-04-08 Added "partitions" to index details. A bit better order of columns for index info.
-- 2023-12-03 Fragmentation removed because it is too slow for big tables to show.
-- 2023-12-09 Bugfix: Case-sensitive instances would throw error.
-- 2023-12-29 Bugfix: works on SQL2008R2
BEGIN
	DECLARE @debug BIT = 0
SET NOCOUNT ON;
SET ARITHABORT, CONCAT_NULL_YIELDS_NULL ON -- without this you can get "SELECT failed because the following SET options have incorrect settings: ..."
SET STATISTICS XML, IO, TIME, PROFILE OFF -- faster to skip showing exec plan, if from outside actual exec plans were "ON"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; -- otherwise proc would fail if outside you set eg, snapshot level which is not allowed on tempdb.

DECLARE @db_id int; SET @db_id = DB_ID()
DECLARE @db_name sysname; SET @db_name = DB_NAME(@db_id)

-- Procedure called with key shortcut CANNOT have more than 1 parameter - otherwise FAILS if dot is present in string ("schema.object"). A bug in SSMS it seems.
-- Toggle between "detailed" and normal mode. Detailed = if you call 2x subsequently this procedure within few seconds with same search.
DECLARE @Extended BIT=0;
-- In Azure, creating normal tables in tempdb fails. Therefore we use global temp tables.
IF OBJECT_ID('tempdb.dbo.##xdetails_state') IS NULL CREATE TABLE ##xdetails_state(session_id INT PRIMARY KEY, last_time DATETIME2(2) NOT NULL, search NVARCHAR(300));
IF EXISTS(SELECT 1 FROM ##xdetails_state s WHERE s.session_id = @@SPID AND s.search=@search AND s.last_time > DATEADD(SECOND, -5, SYSUTCDATETIME())) SET @Extended = 1;
MERGE INTO ##xdetails_state t USING (SELECT session_id = @@SPID, search=@search, last_time=SYSUTCDATETIME()) as src ON t.session_id = src.session_id
WHEN MATCHED THEN UPDATE SET t.search=src.search, last_time=src.last_time
WHEN NOT MATCHED BY TARGET THEN INSERT(session_id, search, last_time) VALUES(src.session_id, src.search, src.last_time)
;

DECLARE @id int; SET @id = OBJECT_ID(@search)
IF @id IS NULL BEGIN
	IF LEFT(@search, 1)='[' AND RIGHT(@search, 1)=']' AND CHARINDEX('.', @search) = 0 SET @search = SUBSTRING(@search, 2, LEN(@search)-2); -- remove brackets from ends
	SELECT @id = o.object_id FROM sys.all_objects o WHERE o.name = @search -- OBJECT_ID returns NULL for subobjects (eg. triggers), but they exist with exact name in sys.objects
END
DECLARE @name sysname, @type_desc sysname, @schema_name sysname;

IF @id IS NOT NULL BEGIN
	SELECT @name = o.name, @type_desc = o.type_desc FROM sys.all_objects o WHERE o.object_id = @id
	SET @schema_name = OBJECT_SCHEMA_NAME(@id)
END
--SELECT @name, @type_desc, @schema_name -- DEBUG

IF @id IS NULL BEGIN -- Search
	IF LEFT(@search, 1)<>'%' AND RIGHT(@search, 1)<>'%' SET @search = '%' + RTRIM(LTRIM(REPLACE(@search, '_', '\_'))) + '%'
	SELECT --TOP 50 
		object_name = schema_name(o.schema_id)+'.'+o.name,
		o.type_desc,
		o.modify_date,
		o.is_ms_shipped,
		m.definition
	INTO #search_result
	FROM sys.all_objects o
		LEFT JOIN sys.all_sql_modules m on o.object_id = m.object_id
	WHERE 1=1
		AND o.parent_object_id = 0 -- skip constraints etc.
		--AND o.is_ms_shipped = 0
		AND o.name LIKE @search ESCAPE '\' --<< SEARCH CRITERIA
		--AND m.definition like '%OBJ%' --<< SEARCH CRITERIA
	IF @@ROWCOUNT = 0 PRINT ''''+ @search + ''' does not exist'
	ELSE SELECT * FROM #search_result ORDER BY 1
	RETURN
END

PRINT +@schema_name+'.'+@name+' ('+@type_desc+')';
PRINT REPLICATE('-', 4);

-----------------------

IF @type_desc='SYNONYM' BEGIN
	SELECT synonym_name = sh.name+'.'+s.name, s.base_object_name, s.modify_date
	FROM sys.synonyms s
		JOIN sys.schemas sh ON sh.schema_id = s.schema_id
	WHERE s.object_id = @id

	RETURN;
END
--/* SQL2012+
IF @type_desc='SEQUENCE_OBJECT' BEGIN
	SELECT sequence_name = sh.name+'.'+s.name,
		current_value = REPLACE(CONVERT(varchar(20), CONVERT(money, s.current_value), 1), '.00', ''),
		start_value = REPLACE(CONVERT(varchar(20), CONVERT(money, s.start_value), 1), '.00', ''),
		s.increment, [type] = t.name,
		cache = CASE WHEN s.is_cached = 1 THEN CONVERT(VARCHAR(10), s.cache_size) ELSE 'no' END,
		s.is_cycling, s.minimum_value, s.maximum_value, s.modify_date, s.is_exhausted
	FROM sys.sequences s
		JOIN sys.schemas sh ON sh.schema_id = s.schema_id
		JOIN sys.types t ON s.user_type_id = t.user_type_id
	WHERE s.object_id = @id
	RETURN;
END
--*/

IF OBJECT_ID('tempdb.dbo.#xdetails_columns') IS NOT NULL DROP TABLE #xdetails_columns
;WITH uqcols_cte AS
(
	select uq.column_id,
		uq_colsort = case WHEN uq.is_primary_key=1 THEN -1 ELSE uq.index_ordinal END,
		uq_colname =
				CASE WHEN uq.is_primary_key=1 THEN 'PK'
					WHEN uq.is_unique_constraint=1 THEN 'UQ'
					WHEN uq.has_filter=1 THEN 'IXF' -- SQL2008+ <<<<<<<<<---------------
					ELSE 'IX'
				END
			+	CASE WHEN uq.is_primary_key=1 THEN '' ELSE '_'+CHAR(ASCII('A')+uq.index_ordinal-1) END
			+	CASE WHEN uq.key_columns = 1 THEN ''
					ELSE convert(varchar, uq.key_ordinal)
				END
	from
	(
		SELECT ic.column_id,
		ic.key_ordinal,
		ix.is_primary_key, ix.is_unique_constraint, 
		ix.has_filter, -- SQL2008+						<<<<<<<<<---------------
		key_columns = count(*) over(partition by ic.object_id, ic.index_id),
		index_ordinal = DENSE_RANK() OVER(order by ix.is_primary_key, ix.index_id)
		FROM sys.index_columns ic
		join sys.indexes ix on ix.object_id=ic.object_id and ix.index_id = ic.index_id
		where ic.key_ordinal<>0 -- only key columns (no "included" columns)
		and ix.is_unique = 1 -- only unique indexes
		and ic.object_id = @id
	) uq
),
uq_cte AS
(
	SELECT u.column_id, unique_columns_csv = STUFF
	(	(	SELECT ', ' + uu.uq_colname
			FROM uqcols_cte uu
			WHERE uu.column_id = u.column_id
			ORDER BY uu.uq_colsort
			FOR XML PATH(''), TYPE
		).value('.', 'varchar(max)'), 1, 2, ''
	)
	FROM uqcols_cte u
	GROUP BY u.column_id
)
SELECT
--OBJECT_NAME(col.OBJECT_ID), -- zakomentiraj, nije nužan atribut
--col.OBJECT_ID,
[Column name] = col.name,
[Type] = t.name 
+CASE 
	WHEN t.NAME IN ('decimal','numeric') THEN '('+convert(VARCHAR,col.precision)+','+convert(VARCHAR,col.scale)+')'
	WHEN t.NAME IN ('time','datetime2','datetimeoffset') THEN '('+convert(VARCHAR,col.scale)+')'
	WHEN col.max_length = -1 AND t.name IN ('varchar','nvarchar','varbinary') THEN '(max)'
	WHEN col.max_length <> -1 AND t.name IN ('nvarchar','nchar') THEN '('+convert(VARCHAR,col.max_length/2)+')'
	WHEN col.max_length <> -1 AND t.name IN ('char','varchar','binary','varbinary') THEN '('+convert(VARCHAR,col.max_length)+')'
	ELSE ''
END,
[References] = STUFF
(	(	-- FK
		SELECT DISTINCT ', '+refsch.name+'.'+refobj.name+'('+refcol.name+')'
		FROM sys.foreign_key_columns fkc
			LEFT JOIN sys.all_objects AS refobj ON refobj.OBJECT_ID = fkc.referenced_object_id
			LEFT JOIN sys.schemas AS refsch ON refobj.schema_id = refsch.schema_id
			LEFT JOIN sys.all_columns AS refcol ON refcol.object_id = refobj.object_id AND refcol.column_id = fkc.referenced_column_id
		WHERE fkc.parent_object_id = @id
			AND fkc.parent_column_id = col.column_id -- external id
		FOR XML PATH(''), TYPE
	).value('.', 'varchar(max)'), 1, 2, ''
),
[Description] = prop.value,
[Default] = SUBSTRING(dc.definition,2,LEN(dc.definition)-2),
[Not null] = ~col.is_nullable,
[PK] = pkcol.key_ordinal, --pkcol.index_column_id
[Unique] = u.unique_columns_csv,
--[Identity] = col.is_identity,
[Identity] = '('+convert(varchar, idc.seed_value)+','+convert(varchar, idc.increment_value)+')'+isnull('=' 
  + replace(convert(varchar(20), CONVERT(MONEY, idc.last_value), 1), '.00', ''), ''),
[Computed] = SUBSTRING(cpc.definition,2,LEN(cpc.definition)-2),
--[Persisted] = CASE cpc.is_persisted WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE NULL END,
[Is marked as] = STUFF
	(	CASE WHEN col.is_rowguidcol=1 THEN ', ROWGUIDCOL' ELSE '' END
		+ CASE WHEN col.is_filestream=1 THEN ', FILESTREAM' ELSE '' END
		+ CASE WHEN cpc.is_persisted=1 THEN ', PERSISTED' ELSE '' END
		+ CASE WHEN col.is_sparse=1 THEN ', SPARSE' ELSE '' END
		+ CASE WHEN col.is_column_set=1 THEN ', COLUMN_SET' ELSE '' END
		+ CASE WHEN col.is_ansi_padded=0 AND t.name IN ('char', 'varchar', 'binary', 'varbinary', 'nchar', 'nvarchar') THEN ', ANSI_PADDING OFF' ELSE '' END
		, 1, 2, ''
	),
[Collation] = 
CASE col.collation_name
	WHEN db.DefaultCollation then 'database_default ('+col.collation_name+')'
	WHEN null then NULL
	ELSE col.collation_name
END,
[Id] = col.column_id
INTO #xdetails_columns
FROM sys.all_columns col
JOIN sys.types t ON col.user_type_id = t.user_type_id
CROSS JOIN
(	select DefaultCollation = convert(sysname,DatabasePropertyEx(@db_name,'Collation'))
) db
-- Komentari
LEFT JOIN sys.extended_properties prop ON prop.major_id = col.object_id
	AND prop.minor_id=col.column_id
	AND prop.NAME = 'MS_Description' -- komentar/opis atributa
	AND prop.class_desc = 'OBJECT_OR_COLUMN' -- same column can be commented in each index also, so we restrict here
-- PK Indeksi
LEFT JOIN sys.indexes pk 
	ON pk.object_id=col.object_id 
	AND pk.is_primary_key = 1
LEFT JOIN sys.index_columns pkcol 
	ON pk.object_id = pkcol.object_id 
	AND pk.index_id = pkcol.index_id 
	AND pkcol.column_id=col.column_id
-- FK
LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = @id and dc.parent_column_id = col.column_id
LEFT JOIN sys.computed_columns cpc ON cpc.object_id = @id AND cpc.column_id = col.column_id
LEFT JOIN sys.identity_columns idc ON idc.object_id = @id AND idc.column_id = col.column_id
LEFT JOIN uq_cte u on u.column_id = col.column_id -- unique
WHERE col.[object_id]=@id
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_columns ORDER BY CASE WHEN [Identity] IS NULL THEN 1 ELSE 0 END, ISNULL(PK, 99), Id

DECLARE @server_fill_factor tinyint SET @server_fill_factor = 100
SELECT @server_fill_factor = ISNULL(NULLIF(CONVERT(tinyint, c.value), 0), 100) FROM sys.configurations c WHERE c.name = 'fill factor (%)'

IF OBJECT_ID('tempdb.dbo.#xdetails_indexes') IS NOT NULL DROP TABLE #xdetails_indexes
-- TODO: filegroup, columnstore
SELECT --s.table_name, 
	[Index name] = s.index_name,
	[Key columns] = SUBSTRING(keycols.lista_atributa,3,8000), -- moze ih biti max 16 i u u svakom retku smiju zajendo zauzeti max 900 bajtova, ne smiju biti LOB tipa (text,image,varchar(max),varbinary(max)...) - sql2000 do sql2008
	[Included columns] = SUBSTRING(inccols.lista_atributa,3,8000), -- ne ubrajaju se u ogranicenje od 16 kolona niti 900 bajtova niti je ogranicen tip podataka (mozes koristiti varchar(max) i sl.).
	[Unique] = si.is_unique,
	[Constraint] = CASE WHEN si.is_primary_key=1 THEN 'PRIMARY KEY' WHEN si.is_unique_constraint=1 THEN 'UNIQUE' END,
	[Type] = si.type_desc,
	--[Clustered] = convert( bit, CASE s.structure_type WHEN 'CLUSTERED' THEN 1 ELSE 0 END ),
	[Rows] = REPLACE(CONVERT(varchar(20), $1*s.rows, 1), '.00', ''),
	[Total KB] = REPLACE(CONVERT(varchar(20), $1*s.reserved_kb, 1), '.00', ''),
--***********2008+
	[Filter] = substring(si.filter_definition, 2, len(si.filter_definition)-2),
--***********
	[Compression] = NULLIF(s.compression_desc, 'NONE'),
	data_space = ds.name,
	s.partitions,
	--[Data KB] = s.data_kb,
	--[Index KB] = s.index_kb, 
	--[Unused KB] = s.unused_kb,
	[Fill factor (leaf nodes)] = case when ff.fill_factor=@server_fill_factor then 'default ('+CONVERT(varchar, @server_fill_factor)+')' else convert(varchar, ff.fill_factor) end,
	[Apply fill factor to non-leaf nodes] = si.is_padded,
	[Disabled] = si.is_disabled,
	[Skip duplicates] = si.ignore_dup_key,
	[Forbid row locks] = ~si.allow_row_locks,
	[Forbid page locks] = ~si.allow_page_locks,
	[Is hypothetical] = si.is_hypothetical,
	[Index id] = si.index_id,
	[Create script] =
	CASE WHEN si.is_primary_key=1 OR si.is_unique_constraint=1 THEN
		'ALTER TABLE '+QUOTENAME(sch.name)+'.'+QUOTENAME(o.name)
		+' ADD CONSTRAINT '+QUOTENAME(si.name)+CASE WHEN si.is_primary_key=1 THEN ' PRIMARY KEY ' ELSE ' UNIQUE ' END+si.type_desc
		+'(['+REPLACE(SUBSTRING(keycols.lista_atributa,3,8000), ', ', '], [')+'])'
	ELSE
		'CREATE '+CASE WHEN si.is_unique=1 then 'UNIQUE ' else '' END+si.type_desc+' INDEX '+QUOTENAME(si.name)
		+' ON '+QUOTENAME(sch.name)+'.'+QUOTENAME(o.name)
		+ISNULL
		(	'(['+REPLACE(SUBSTRING(keycols.lista_atributa,3,8000), ', ', '], [')+'])'
			+ISNULL(' INCLUDE(['+REPLACE(SUBSTRING(inccols.lista_atributa,3,8000), ', ', '], [')+'])', ''),
			''
		)+ISNULL(' WHERE '+ff.filter, '')
		COLLATE DATABASE_DEFAULT
	END
	+CASE
	WHEN	si.type_desc LIKE '%COLUMNSTORE%' THEN '' -- columnstore does not allow FILLFACTOR and SORT_IN_TEMPDB options
		ELSE ' WITH(FILLFACTOR = '+CONVERT(varchar(3), CASE WHEN si.fill_factor=0 THEN 100 ELSE si.fill_factor END)
		+ISNULL(', DATA_COMPRESSION = '+NULLIF(s.compression_desc, 'NONE'), '')
		+ISNULL(', IGNORE_DUP_KEY = '+CASE WHEN si.ignore_dup_key=1 THEN 'ON' ELSE NULL END, '')
		+ISNULL(', ALLOW_ROW_LOCKS = '+CASE WHEN si.allow_row_locks=0 THEN 'OFF' ELSE NULL END, '')
		+ISNULL(', ALLOW_PAGE_LOCKS = '+CASE WHEN si.allow_page_locks=0 THEN 'OFF' ELSE NULL END, '')
		+ISNULL(', PAD_INDEX = '+CASE WHEN si.is_padded=1 THEN 'ON' ELSE NULL END, '')
		+', SORT_IN_TEMPDB=ON)'
	END + ';'
INTO #xdetails_indexes
FROM 
	sys.indexes si
	JOIN sys.all_objects o ON o.object_id = si.object_id
	JOIN sys.schemas sch ON sch.schema_id = o.schema_id
	JOIN sys.data_spaces ds ON ds.data_space_id = si.data_space_id -- filegroup or partition scheme
	CROSS APPLY
	(	SELECT fill_factor = ISNULL(NULLIF(si.fill_factor, 0), 100),
			filter = SUBSTRING(si.filter_definition, 2, LEN(si.filter_definition)-2) -- remove parentheses. 2008+
	) ff
	JOIN
	(
		select -- testiraj na XML-Index and FT-Index. Testiraj broj redaka sa filtriranim indeksima!
			[table_name] = OBJECT_NAME(ix.object_id, @db_id),
			table_id = ix.object_id,
			index_id = ix.index_id,
			index_name = CASE WHEN ix.index_id IN(0, 255) THEN NULL ELSE ix.name end, -- heap i lob nisu indeks, pa umjesto imena tablice ispisujem null.
			structure_type = CASE WHEN ix.index_id = 0 THEN 'HEAP' WHEN ix.index_id = 1 THEN 'CLUSTERED' WHEN ix.index_id = 255 THEN 'LOB' ELSE 'NONCLUSTERED' END,
			[reserved_kb] = 8 * tt.total,
			[rows] = tt.rows,
			[data_kb] = 8 * tt.data,
			[index_kb] = 8 * (tt.used - tt.data),
			[unused_kb] = 8 * (tt.total - tt.used),
			ix.type_desc,
			tt.compression_desc,
			tt.partitions
		FROM
			sys.indexes ix
			LEFT JOIN
			(
				select p.object_id, p.index_id,
				total = sum(a.total_pages),
				rows = sum(CASE WHEN a.type=1 THEN p.rows ELSE 0 END),
				used = sum(a.used_pages),
				data = sum
				(	CASE -- XML-Index and FT-Index internal tables are not considered "data", but is part of "index_size"
						When it.internal_type IN (202,204,211,212,213,214,215,216) Then 0
						When a.type <> 1 Then a.used_pages
						When p.index_id < 2 Then a.data_pages
						Else 0
					END
				),
				compression_desc = MAX(p.data_compression_desc),
				partitions = MAX(p.partition_number)
				from sys.partitions p
					join sys.allocation_units a on p.partition_id = a.container_id
					left join sys.internal_tables it on p.object_id = it.object_id
				where p.object_id = @id
				group by p.object_id, p.index_id
			) tt
			ON tt.object_id = ix.object_id AND tt.index_id = ix.index_id -- samo radi naziva indeksa
		WHERE ix.object_id = @id
	) s ON si.object_id=s.table_id AND si.index_id=s.index_id
	CROSS APPLY
	(	-- key columns
		SELECT ', '+col.name + CASE WHEN ik.is_descending_key=1 THEN ' DESC' ELSE '' END
		from sys.index_columns ik
		JOIN sys.all_columns col ON col.[OBJECT_ID] = ik.[object_id]
			AND col.column_id = ik.column_id
		WHERE ik.[object_id] = s.table_id
		AND ik.index_id = s.index_id
		AND ik.is_included_column = 0 -- key columns
		ORDER BY ik.key_ordinal
		FOR XML PATH('')
	) keycols(lista_atributa)
	CROSS APPLY
	(	-- included columns
		SELECT ', '+col.name
		from sys.index_columns ik
		JOIN sys.all_columns col ON col.[OBJECT_ID] = ik.[object_id]
			AND col.column_id = ik.column_id
		WHERE ik.[object_id] = s.table_id
		AND ik.index_id = s.index_id
		AND ik.is_included_column = 1 -- included columns
		ORDER BY ik.key_ordinal
		FOR XML PATH('')
	) inccols(lista_atributa)
WHERE s.table_id = @id
	--AND s.index_id >= 1 -- no heaps
--***********2005-
	--AND s.index_id <= 254 -- only indexes (clustered and nonclustered)
--***********
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_indexes ORDER BY [Key columns]
ELSE PRINT 'No indexes'

IF OBJECT_ID('tempdb.dbo.#xdetails_check') IS NOT NULL DROP TABLE #xdetails_check
SELECT [Check Constraint] = ck.name,
	Definition,
	[Enabled] = ~is_disabled, 
	[Trusted] = ~is_not_trusted, 
	[For replication] = ~is_not_for_replication
INTO #xdetails_check
FROM sys.check_constraints ck
WHERE ck.parent_object_id = @id
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_check ORDER BY 1
ELSE PRINT 'No check constraints'

-- Triggers
IF OBJECT_ID('tempdb.dbo.#xdetails_triggers') IS NOT NULL DROP TABLE #xdetails_triggers
SELECT [Trigger name] = tr.name,
	[Enabled] = ~tr.is_disabled,
	[Fires] = CASE WHEN tr.is_instead_of_trigger=1 THEN 'INSTEAD OF' ELSE 'AFTER' END,
	[Events] = stuff
	(	(	SELECT [text()] = ', ' 
				+ CASE WHEN te.is_first=1 THEN 'FIRST ON ' ELSE '' END
				+ CASE WHEN te.is_last=1 THEN 'LAST ON ' ELSE '' END
				+ te.type_desc
			FROM sys.trigger_events te
			WHERE te.object_id = tr.Object_id
			ORDER BY te.type
			FOR XML PATH('') -- ovaj red zakomentiraj pa vidi rezultat
		), 1, 2, '' -- mices prvi zarez
	),
	[For replication] = ~tr.is_not_for_replication,
	m.definition,
	[Created] = tr.create_date,
	[Modified] = tr.modify_date,
	[Type] = tr.type_desc +' ON '+tr.parent_class_desc
INTO #xdetails_triggers
FROM sys.triggers tr
	LEFT JOIN sys.all_sql_modules m ON m.object_id = tr.object_id
WHERE tr.parent_id = @id
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_triggers ORDER BY 1
ELSE PRINT 'No triggers'


-- Child tables
IF OBJECT_ID('tempdb.dbo.#xdetails_childs') IS NOT NULL DROP TABLE #xdetails_childs
SELECT
[Child table] = cs.name+'.'+co.name, -- fk_table
[Parent table] = ps.name+'.'+po.name,
[Child column(s)] = childs.lista_atributa, -- fk_columns
[Parent column(s)] = parents.lista_atributa,
--
[Enabled] = ~fk.is_disabled,
[Trusted] = ~fk.is_not_trusted,
[For replication] = ~fk.is_not_for_replication,
[FK has Index] = -- one or more indexes exist on child table with key beginning with fk columns of that fk.
(	SELECT COUNT(*)
	FROM sys.indexes ix
	WHERE ix.object_id = fk.parent_object_id --=== fk.parent_object_id is "child table id"
		AND -- key_col_ids, comma separated
		(	SELECT CONVERT(VARCHAR, ic.column_id)+','
			FROM sys.index_columns ic
			WHERE ic.object_id = ix.object_id
				AND ic.is_included_column = 0 -- key columns
				AND ic.index_id = ix.index_id
			ORDER BY ic.key_ordinal
			FOR XML PATH('')
		)
		LIKE
		(	SELECT -- Columns (column ids) of that fk, comma separated
				CONVERT(VARCHAR, fkcol.parent_column_id)+','
			FROM sys.foreign_key_columns fkcol
			WHERE fkcol.parent_object_id = fk.parent_object_id --=== fk.parent_object_id is "child table id"
				AND fkcol.constraint_object_id = fk.object_id --=== id of fk itself
			ORDER BY fkcol.constraint_column_id
			FOR XML PATH('')
		)+'%'
),
[On delete] = REPLACE(fk.delete_referential_action_desc,'_',' '), -- del_action
[On update] = REPLACE(fk.update_referential_action_desc,'_',' '), -- upd_action
[Foreign key name] = fk.name,
[Drop script] = 'ALTER TABLE ['+cs.name+'].['+co.name+'] DROP CONSTRAINT ['+fk.name+']',
[Create script (without options)] = 'ALTER TABLE ['+cs.name+'].['+co.name+'] ADD CONSTRAINT ['+fk.name+'] FOREIGN KEY('+childs.lista_atributa
	+') REFERENCES ['+ps.name+'].['+po.name+'] ('+parents.lista_atributa+')'
	+CASE WHEN fk.delete_referential_action_desc='NO_ACTION' THEN '' ELSE ' ON DELETE '+REPLACE(fk.delete_referential_action_desc,'_',' ') END
	+CASE WHEN fk.update_referential_action_desc='NO_ACTION' THEN '' ELSE ' ON UPDATE '+REPLACE(fk.update_referential_action_desc,'_',' ') END
	COLLATE DATABASE_DEFAULT,
object_id = fk.parent_object_id, -- child table id
fk.referenced_object_id -- parent table id
INTO #xdetails_childs
FROM sys.foreign_keys fk -- FK name, is enabled, etc.
CROSS APPLY
(	SELECT lista_atributa = STUFF
		(	(	SELECT ', ', col.name
	from sys.foreign_key_columns fkcol
	JOIN sys.all_columns col ON col.object_id = fkcol.parent_object_id 
		AND col.column_id = fkcol.parent_column_id
	WHERE fkcol.constraint_object_id = fk.object_id
	ORDER BY fkcol.constraint_column_id
				FOR XML PATH(''), TYPE
			).value('.', 'varchar(max)'), 1, 2, ''
		)
) childs
CROSS APPLY
(	SELECT lista_atributa = STUFF
		(	(	SELECT ', ', refcol.name
				from sys.foreign_key_columns fkcol
				JOIN sys.all_columns refcol ON refcol.object_id = fkcol.referenced_object_id 
					AND refcol.column_id = fkcol.referenced_column_id
				WHERE fkcol.constraint_object_id = fk.object_id
				ORDER BY fkcol.constraint_column_id
				FOR XML PATH(''), TYPE
			).value('.', 'varchar(max)'), 1, 2, ''
		)
) parents
JOIN sys.all_objects co ON co.object_id=fk.parent_object_id
JOIN sys.schemas cs ON cs.schema_id=co.schema_id
JOIN sys.all_objects po ON po.object_id=fk.referenced_object_id
JOIN sys.schemas ps ON ps.schema_id=po.schema_id
WHERE (fk.referenced_object_id = @id OR fk.parent_object_id = @id)
IF EXISTS(SELECT 1 FROM #xdetails_childs WHERE referenced_object_id = @id) SELECT * FROM #xdetails_childs WHERE referenced_object_id = @id ORDER BY 1,2
ELSE PRINT 'No child tables'


IF OBJECT_ID('tempdb.dbo.#xdetails_code') IS NOT NULL DROP TABLE #xdetails_code
SELECT m.definition, o.type_desc, o.modify_date, o.create_date, schema_name=SCHEMA_NAME(o.schema_id), parent_object=OBJECT_NAME(parent_object_id, @db_id)
INTO #xdetails_code
FROM sys.all_sql_modules m
	JOIN sys.all_objects o ON o.object_id = m.object_id
WHERE m.object_id = @id
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_code;


IF OBJECT_ID('tempdb.dbo.#xdetails_params') IS NOT NULL DROP TABLE #xdetails_params
SELECT
	param_name = CASE WHEN p.name='' THEN 'RETURNS' ELSE p.name END,
	[type] = ISNULL(NULLIF(ts.name, 'sys') + '.', '') + t.name 
		+CASE 
			WHEN t.NAME IN ('decimal','numeric') THEN '('+convert(VARCHAR,p.precision)+','+convert(VARCHAR,p.scale)+')'
			WHEN t.NAME IN ('time','datetime2','datetimeoffset') THEN '('+convert(VARCHAR,p.scale)+')'
			WHEN p.max_length = -1 AND t.name IN ('varchar','nvarchar','varbinary') THEN '(max)'
			WHEN p.max_length <> -1 AND t.name IN ('nvarchar','nchar') THEN '('+convert(VARCHAR,p.max_length/2)+')'
			WHEN p.max_length <> -1 AND t.name IN ('char','varchar','binary','varbinary') THEN '('+convert(VARCHAR,p.max_length)+')'
			ELSE ''
		END,
	p.is_output, p.is_readonly,
	-- p.default_value, p.has_default_value, -- always null, 0, therefore left out.
	t.collation_name, p.parameter_id
INTO #xdetails_params
FROM sys.all_parameters p
	JOIN sys.types t ON p.user_type_id = t.user_type_id
	JOIN sys.schemas ts ON ts.schema_id = t.schema_id
WHERE p.object_id = @id
IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_params p ORDER BY CASE WHEN p.parameter_id=0 THEN 99999 ELSE p.parameter_id END;


IF @Extended=1 BEGIN
	-- Parent tables
	IF EXISTS(SELECT 1 FROM #xdetails_childs WHERE object_id = @id) SELECT * FROM #xdetails_childs WHERE object_id = @id ORDER BY 1,2
	ELSE PRINT 'No referenced (parent) tables'

	-- Total size, partition count, compression
	select
	[Object name] = CASE WHEN GROUPING(t.[Object name]) = 1 THEN OBJECT_NAME(MAX(t.table_id), @db_id) ELSE t.[Object name] END,
	[Structure type] = CASE WHEN GROUPING(t.[Object name]) = 1 THEN 'TOTAL ===>>>' ELSE MAX(t.[Structure type]) END,
	[Total KB] = REPLACE(CONVERT(varchar(20), $1*SUM(t.[Total KB]), 1), '.00', ''),
	[Rows] = REPLACE(CONVERT(varchar(20), $1*MAX(t.[Rows]), 1), '.00', ''),
	[Data KB] = REPLACE(CONVERT(varchar(20), $1*SUM(t.[Table KB]), 1), '.00', ''),
	[Index KB] = REPLACE(CONVERT(varchar(20), $1*SUM(t.[Index KB]), 1), '.00', ''),
	[Unused KB] = REPLACE(CONVERT(varchar(20), $1*SUM(t.[Unused KB]), 1), '.00', ''),
	[Compressed] = MAX(t.[Compression]), -- SQL2008+
	[Filegroup] = MAX(t.[Filegroup]),
	[Partitioned] = MAX(t.[Partitioned]),
	[Partitioned by] = MAX(t.[Partitioned by]),
	[Partitions] = MAX(t.[Partitions]),
	[Partition scheme] = MAX(t.[Partition scheme]),
	[Partition function] = MAX(t.[Partition function])
	from
	(	select -- test on XML-Index and FT-Index. Test row count on filtered indexes
		table_id = tt.object_id,
		[Object name] = CASE WHEN tt.index_id IN(0, 255) THEN OBJECT_NAME(tt.object_id, @db_id) ELSE ix.name end, -- heap and lob are not indexes, therefore writing null instead of name
		[Structure type] = CASE WHEN tt.index_id = 0 THEN 'HEAP' WHEN tt.index_id = 1 THEN 'CLUSTERED' WHEN tt.index_id = 255 THEN 'LOB' ELSE 'NONCLUSTERED' END,
		[Rows] = tt.rows,
		[Total KB] = 8 * tt.total,
		[Table KB] = 8 * tt.data,
		[Index KB] = 8 * (tt.used - tt.data),
		[Unused KB] = 8 * (tt.total - tt.used),
		[Filegroup] = case 
			when ds.type = 'FG' then ds.name 
			ELSE pfg.fg_min + ISNULL('-'+NULLIF(pfg.fg_max,pfg.fg_min),'') + '(' + CONVERT(varchar, pf.fanout) + ')'
		end,
		[Compression] = tt.compression_min + ISNULL('-'+NULLIF(tt.compression_max,tt.compression_min),'') + isnull('(' + CONVERT(varchar, pf.fanout) + ')', ''), -- SQL2008+
		[Partitioned] = case when ds.type = 'PS' then 'YES' else 'NO' end,
		[Partitioned by] = col.name,
		[Partitions] = pf.fanout,
		[Partition scheme] = case when ds.type = 'PS' then ds.name END,
		[Partition function] = pf.name
		from
		(	select p.object_id, p.index_id,
			total = sum(a.total_pages),
			rows = sum(CASE WHEN a.type=1 THEN p.rows ELSE 0 END),
			used = sum(a.used_pages),
			data = sum
			(	CASE -- XML-Index and FT-Index internal tables are not considered "data", but is part of "index_size"
					When it.internal_type IN (202,204,211,212,213,214,215,216) Then 0
					When a.type <> 1 Then a.used_pages
					When p.index_id < 2 Then a.data_pages
					Else 0
				END
			),
			compression_min = min(p.data_compression_desc), -- SQL2008+
			compression_max = max(p.data_compression_desc) -- SQL2008+
			from sys.partitions p
			join sys.allocation_units a on p.partition_id = a.container_id
			left join sys.internal_tables it on p.object_id = it.object_id
			where p.object_id = @id
			group by p.object_id, p.index_id
		) tt
		join sys.indexes ix on tt.object_id = ix.object_id and tt.index_id = ix.index_id -- samo radi naziva indeksa
		join sys.data_spaces ds ON ds.data_space_id = ix.data_space_id -- za naziv data space-a
		left join sys.partition_schemes ps ON ix.data_space_id = ps.data_space_id -- radi partition function
		left join sys.partition_functions pf ON ps.function_id = pf.function_id
		left join -- min i max partition filegroup, za svaku partition schemu
		(	SELECT dds.partition_scheme_id, fg_min = MIN(ds2.name), fg_max = MAX(ds2.name)
			from sys.destination_data_spaces dds -- za svaku particiju sheme kaže u koju filegrupu je smještena
			join sys.data_spaces ds2 ON ds2.data_space_id = dds.data_space_id -- za naziv data space-a
			GROUP BY dds.partition_scheme_id
		) pfg ON pfg.partition_scheme_id = ps.data_space_id
		left join sys.index_columns pc ON pc.object_id = tt.object_id AND pc.index_id = tt.index_id AND pc.partition_ordinal = 1 -- po kojem polju je particionirano
		left join sys.columns col ON col.object_id = pc.object_id AND col.column_id = pc.column_id -- za naziv kolone
	) t
	GROUP BY t.[Object name] WITH ROLLUP
	ORDER BY SUM(t.[Total KB]) DESC, GROUPING(t.[Object name]) desc

--/* SQL2012+
	-- Partitions
	IF OBJECT_ID('tempdb.dbo.#partitions') IS NOT NULL DROP TABLE #partitions
	SELECT -- testiraj sa LEFT i RIGHT part fjama, particioniranim XML-Index and FT-Index. Testiraj broj redaka sa filtriranim part indeksima!
	[Index name] = CASE WHEN tt.index_id IN(0, 255) THEN OBJECT_NAME(tt.object_id, @db_id) ELSE ix.name end, -- heap i lob nisu indeks, pa umjesto imena tablice ispisujem null.
	[Structure type] = CASE WHEN tt.index_id = 0 THEN 'HEAP' WHEN tt.index_id = 1 THEN 'CLUSTERED' WHEN tt.index_id = 255 THEN 'LOB' ELSE 'NONCLUSTERED' END,
	[Partition] = tt.partition_number,
	[Condition] = col.name + ' ' + SUBSTRING('<>', 1 + pf.boundary_value_on_right, 1) + '= ' 
	+	CASE WHEN prv.VALUE IS NULL THEN SUBSTRING('-',0 + pf.boundary_value_on_right,1) + 'infinity'
			ELSE CONVERT(varchar,prv.VALUE,CASE WHEN CONVERT(varchar, SQL_VARIANT_PROPERTY(prv.VALUE , 'BaseType')) LIKE '%date%' THEN 121 ELSE 0 END )
		END,
	[Rows] = tt.rows,
	[Total KB] = 8 * tt.total,
	[Compression] = tt.data_compression_desc, --!!! SQL2008
	[Filegroup] = ds.name,
	[Partition scheme] = ps.name,
	[Partition function] = pf.NAME,
	[Data KB] = 8 * tt.data,
	[Index KB] = 8 * (tt.used - tt.data),
	[Unused KB] = 8 * (tt.total - tt.used)
	--[Fragmentation pct] = ROUND(pst.avg_fragmentation_in_percent, 2)
	INTO #partitions
	from
	(	select p.object_id, p.index_id, p.partition_number,
		total = sum(a.total_pages),
		rows = max(p.rows),
		used = sum(a.used_pages),
		data_compression_desc = max(p.data_compression_desc), --!!! SQL2008 -- there is only one compression type in one partition
		data = sum
		(	CASE -- XML-Index and FT-Index internal tables are not considered "data", but is part of "index_size"
				When it.internal_type IN (202,204,211,212,213,214,215,216) Then 0
				When a.type <> 1 Then a.used_pages
				When p.index_id < 2 Then a.data_pages
				Else 0
			END
		)
		from sys.partitions p
		join sys.allocation_units a on p.partition_id = a.container_id
		left join sys.internal_tables it on p.object_id = it.object_id
		where p.object_id = @id
		group by p.object_id, p.index_id, p.partition_number--, p.data_compression_desc
	) tt
	--OUTER APPLY sys.dm_db_index_physical_stats(@db_id, @id, tt.index_id, tt.partition_number, 'LIMITED') pst -- WARNING: fails with error for disabled indexes! SLOW!
	join sys.indexes ix on tt.object_id = ix.object_id and tt.index_id = ix.index_id -- samo radi naziva indeksa
	join sys.partition_schemes ps ON ix.data_space_id = ps.data_space_id -- radi partition function
	join sys.partition_functions pf ON ps.function_id = pf.function_id
	join sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id AND dds.destination_id = tt.partition_number -- za svaku particiju sheme kaže u koju filegrupu je smještena
	join sys.data_spaces ds ON ds.data_space_id = dds.data_space_id -- za naziv data space-a
	join sys.index_columns pc ON pc.object_id = tt.object_id AND pc.index_id = tt.index_id AND pc.partition_ordinal = 1 -- po kojem polju je particionirano
	join sys.columns col ON col.object_id = pc.object_id AND col.column_id = pc.column_id -- za naziv kolone
	left join sys.partition_range_values prv ON prv.function_id = ps.function_id 
		and tt.partition_number = prv.boundary_id + pf.boundary_value_on_right
	--WHERE (pst.index_depth > 0 OR pst.index_depth IS NULL) -- columnstore creates duplicate with depth 0
	--WHERE -- if columnstore disregard delta stores and segmentations
	--	(	tt.data_compression_desc like 'COLUMNSTORE%' and pst.index_depth=0
	--		or tt.data_compression_desc NOT like 'COLUMNSTORE%' and pst.index_depth>0
	--		or tt.rows=0
	--	)
	IF @@ROWCOUNT > 0 SELECT * FROM #partitions ORDER BY [Index name], [Partition] DESC
--*/

	IF @type_desc LIKE '%TABLE' OR @type_desc = 'VIEW' -- because error is thrown by sys functions used inside, if object is of different type
	BEGIN
		-- Index usage in plans
		IF OBJECT_ID('tempdb.dbo.#xdetails_index_usage_from_plan') IS NOT NULL DROP TABLE #xdetails_index_usage_from_plan
		SELECT
			[Index name] = ISNULL(i.name, OBJECT_NAME(i.object_id, @db_id)),
			t.[Key columns],
			[Last lookup] = s.last_user_lookup,
			[Last seek] = s.last_user_seek,
			[Last scan] = s.last_user_scan,
			[Last update] = s.last_user_update,
			[Table Lookups (equality by CL key or RID)] = s.user_lookups,
			[Seeks (range or equality)] = s.user_seeks,
			[Scans (full or top)] = s.user_scans,
			[Updates] = s.user_updates,
			[Reads per read&write %] = CASE
					WHEN ISNULL(s.user_lookups+s.user_seeks+s.user_scans+s.user_updates, 0) = 0 THEN NULL
					ELSE CONVERT(int, $100*(s.user_lookups+s.user_seeks+s.user_scans)/(s.user_lookups+s.user_seeks+s.user_scans+s.user_updates))
				END
		INTO #xdetails_index_usage_from_plan
		FROM sys.indexes i
			LEFT JOIN sys.dm_db_index_usage_stats s on s.object_id = i.object_id
				AND s.index_id = i.index_id
				AND s.database_id = @db_id
			join #xdetails_indexes t ON t.[Index id] = i.index_id
		WHERE i.object_id = @id
			AND i.is_hypothetical = 0 -- makni hipoteticke indekse koji ne zauzimaju prostor (nema ih)
		IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_index_usage_from_plan ORDER BY [Key columns], [Index name]

		-- Index actual usage
		IF OBJECT_ID('tempdb.dbo.#xdetails_index_usage_actual') IS NOT NULL DROP TABLE #xdetails_index_usage_actual
		SELECT 
			[Index name]=ix.name,
			t.[Key columns],
			[Partition] = os.partition_number,
			f.*,
			[Seeks and Scans] = os.range_scan_count, -- scans: ordered or not ordered, full or top. Seek: range, or equality on nonunique.
			[Lookups by Unique key] = os.singleton_lookup_count, -- equality seek on UNIQUE index (NC or CL), or RID lookup on HEAP.
			[Rows inserted]=os.leaf_insert_count,
			[Rows updated]=os.leaf_update_count,
			[Rows deleted]=os.leaf_delete_count,
			[Rows delete pending (ghost)]=os.leaf_ghost_count,
			[Index id]=os.index_id
		INTO #xdetails_index_usage_actual
		FROM sys.dm_db_index_operational_stats(@db_id, @id, DEFAULT, DEFAULT) os
			cross apply
			(	SELECT [Rows modified (ins+upd+del)] = os.leaf_insert_count+os.leaf_update_count+os.leaf_delete_count+os.leaf_ghost_count, -- number of rows changed
					[Read Operations (seeks&scans+lookups)] = os.range_scan_count+os.singleton_lookup_count -- number of read operations
			) f
			join sys.indexes ix on ix.object_id=os.object_id and ix.index_id=os.index_id
			join #xdetails_indexes t ON t.[Index id] = os.index_id
		IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_index_usage_actual ORDER BY [Key columns], [Index name], [Partition]

	END -- if table or view

	-- Statistics
	IF @type_desc NOT LIKE '%FUNCTION%' AND @type_desc NOT LIKE '%PROCEDURE%' -- because error is thrown by DBCC SHOW_STATISTICS
	BEGIN
		DECLARE @sql nvarchar(max); SET @sql=''
		SELECT @sql=@sql+'DBCC SHOW_STATISTICS('''+QUOTENAME(@schema_name)+'.'+QUOTENAME(@name)+''', '''+st.name+''') WITH STAT_HEADER, NO_INFOMSGS;'+CHAR(13)
		FROM sys.stats st
		WHERE st.object_id = @id

		IF OBJECT_ID('tempdb.dbo.#stat_header') IS NOT NULL DROP TABLE #stat_header
		CREATE TABLE #stat_header
		(	StatisticsName sysname, Updated datetime, TotalRows bigint, SampledRows bigint, HistogramSteps int, Density float, AvgKeyLength int, StringIndex varchar(10)
			, Filter varchar(8000) COLLATE DATABASE_DEFAULT, UnfilteredRows bigint --****** SQL2008 up
			, PersistentSamplePct money --**** SQL 2016 and up?
		)
		DECLARE @sqlInsert nvarchar(max); SET @sqlInsert='INSERT INTO #stat_header(
			StatisticsName, Updated, TotalRows, SampledRows, HistogramSteps, Density, AvgKeyLength, StringIndex
		--SQL2008+--, Filter, UnfilteredRows
		--SQL2016+--, PersistentSamplePct
		) EXEC(@sql)'

		DECLARE @ProductVersion AS NVARCHAR(20) = CAST(SERVERPROPERTY ('productversion') AS NVARCHAR(20));
		DECLARE @MajorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 4) AS SMALLINT);
		DECLARE @MinorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 3) AS SMALLINT);
		DECLARE @BuildVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 2) AS SMALLINT);
		DECLARE @Edition VARCHAR(50) =
			CASE ServerProperty('EngineEdition')
				WHEN 1 THEN 'Personal'
				WHEN 2 THEN 'Standard'
				WHEN 3 THEN 'Enterprise'
				WHEN 4 THEN 'Express'
				WHEN 5 THEN 'Azure'
				ELSE 'Unknown'
			END
		DECLARE @compatibilityLevel INT
		SELECT @compatibilityLevel = d.compatibility_level FROM sys.databases d WHERE d.database_id = DB_ID()

		IF @MajorVersion >= 10 SET @sqlInsert = REPLACE(@sqlInsert, '--SQL2008+--', '');
		IF @MajorVersion >= 13 OR @Edition = 'Azure' AND @compatibilityLevel >= 130 SET @sqlInsert = REPLACE(@sqlInsert, '--SQL2016+--', '');
		IF @debug = 1 PRINT '@sql = '+@sql
		IF @debug = 1 PRINT '@sqlInsert = '+@sqlInsert
		EXEC sys.sp_executesql @sqlInsert, N'@sql NVARCHAR(MAX)', @sql;
		IF @@ROWCOUNT > 0 BEGIN
			SELECT h.*, st.user_created, st.no_recompute, st.is_incremental
			FROM #stat_header h
				JOIN sys.stats st ON st.object_id = @id AND h.StatisticsName = st.name COLLATE DATABASE_DEFAULT
			ORDER BY StatisticsName
		END
	END

--/* SQL2012+
	-- Columnstore segments (segment = a column splitted accross compressed row groups)
	SELECT --p.index_id,
		[Columnstore Index] = ix.name, [Partition] = p.partition_number, 
		s.column_id, 
		[Column] = col.name,
		s.segment_id,
		--[Row Count] = IIF(s.column_id = 65535, rg.deleted_rows, s.row_count), 
		s.row_count, --rg.deleted_rows,
		s.min_data_id, s.max_data_id,
		[Size KB] = REPLACE(CONVERT(varchar(20), s.on_disk_size/$1024, 1), '.00', ''),
		--s.has_nulls, s.null_value,
		--[State] = rg.state_description,
		[Encoding] = CASE s.encoding_Type
			WHEN 1 THEN 'VALUE_BASED'
			WHEN 2 THEN 'VALUE_HASH_BASED'
			WHEN 3 THEN 'STRING_HASH_BASED'
			WHEN 4 THEN 'STORE_BY_VALUE_BASED'
			WHEN 5 THEN 'STRING_STORE_BY_VALUE_BASED'
			ELSE CONVERT(VARCHAR, s.encoding_Type)
		END
	INTO #xdetails_columnstore_segments
	FROM sys.column_store_segments s
		JOIN sys.partitions p ON p.hobt_id = s.hobt_id
		JOIN sys.indexes ix ON ix.object_id = p.object_id AND ix.index_id = p.index_id
		--FULL JOIN sys.column_store_row_groups rg ON rg.object_id = p.object_id AND rg.index_id = p.index_id
		--	AND rg.partition_number = p.partition_number AND rg.row_group_id = s.segment_id
		LEFT JOIN sys.all_columns col ON col.object_id = p.object_id AND col.column_id = s.column_id
	WHERE p.object_id = @id
	IF @@ROWCOUNT > 0 SELECT * FROM #xdetails_columnstore_segments ORDER BY [Columnstore Index], [Partition], column_id, segment_id
--*/
END -- IF @Extended=1

END
GO

-- Without this, procedure can be called, but all system views would be from master database (not current user's), except DB_NAME etc functions work from user's perspective!
EXEC sys.sp_MS_marksystemobject 'sp_xdetails' -- sets "is_ms_shipped=1"
GO
