USE master;
DROP DATABASE TestDB
GO
CREATE DATABASE TestDB
GO
ALTER DATABASE TestDB SET RECOVERY SIMPLE;
ALTER DATABASE [TestDB] SET QUERY_STORE = ON
ALTER DATABASE [TestDB] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, INTERVAL_LENGTH_MINUTES = 1, QUERY_CAPTURE_MODE = ALL, DATA_FLUSH_INTERVAL_SECONDS = 300)
GO
USE TestDB;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = ON;
GO


IF OBJECT_ID('dbo.BigChild') IS NOT NULL DROP TABLE dbo.BigChild;
IF OBJECT_ID('dbo.SmallTable') IS NOT NULL DROP TABLE dbo.SmallTable;
GO
CREATE TABLE SmallTable
(
	Id INT IDENTITY CONSTRAINT PK_SmallTable PRIMARY KEY,
	MyGUID UNIQUEIDENTIFIER DEFAULT NEWSEQUENTIALID(),
	DateStart DATE DEFAULT GETDATE(),
	ThisMoment DATETIME2(2) DEFAULT SYSDATETIME(),
	MeetingStart SMALLDATETIME,
	Text_CS2 NVARCHAR(1000) DEFAULT N'abcd'
	--, Text_UTF8 VARCHAR(1000) COLLATE Latin1_General_100_CI_AS_SC_UTF8 DEFAULT N'abcd'
)
GO
INSERT INTO SmallTable DEFAULT VALUES;
GO 6
SELECT * FROM SmallTable
GO

CREATE TABLE BigChild
(
	ChildId INT IDENTITY CONSTRAINT PK_BigChild PRIMARY KEY,
	SomeBytes NCHAR(4000) DEFAULT '_',
	ParentId INT FOREIGN KEY REFERENCES SmallTable(Id)
)
GO
INSERT INTO BigChild(ParentId)
SELECT TOP(100000) 1
FROM sys.all_columns, sys.all_columns c
GO 5
EXEC sp_spaceused 'BigChild' -- 800 MB per 100 000 rows
GO


CHECKPOINT;
DBCC DROPCLEANBUFFERS; -- remove table from RAM
GO

-- Set low max memory, so you do not have to create super-big tables
-- Do NOT do this in production!!!
EXEC sys.sp_configure N'show advanced options', N'1'  RECONFIGURE WITH OVERRIDE
EXEC sys.sp_configure N'max server memory (MB)', N'1500' -- do NOT go too low, otherwise SQL might become inaccessible!
RECONFIGURE WITH OVERRIDE
GO

-- Object Explorer - FILTER tempdb table list to "TABLE" so only SmallTable is seen!

/* REVERT to normal
EXEC sys.sp_configure N'max server memory (MB)', N'6000'
RECONFIGURE WITH OVERRIDE
GO
EXEC sys.sp_configure N'show advanced options', N'0'  RECONFIGURE WITH OVERRIDE

DROP TABLE dbo.BigChild;
*/