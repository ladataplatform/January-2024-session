USE TestDB;
GO


-- IF OBJECT_ID('dbo.SmallTable') IS NOT NULL DROP TABLE dbo.SmallTable;
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
-- Really small table
EXEC sp_spaceused 'SmallTable'
EXEC sp_helpindex 'SmallTable'
GO
SELECT *, 
	CS2_Bytes = DATALENGTH(Text_CS2)
	--, UTF8_Bytes = DATALENGTH(Text_UTF8)
FROM SmallTable
GO


SELECT * FROM SmallTable

-- How long a one-row delete in a small table should take? 1ms? 0ms?
SET STATISTICS TIME ON
DELETE FROM SmallTable WHERE Id = 6; -- id that exists
DELETE FROM SmallTable WHERE Id = 5; -- id that exists


-- Why?







-- See execution plan




-- See table details with sp_xdetails
EXEC sp_xdetails 'SmallTable'
-- https://blog.sqlxdetails.com/sp_xdetails-procedure-instead-of-ssms-addin/



-- Optimization
CREATE INDEX IX_BigChild_ParentId ON dbo.BigChild(ParentId)
GO

-- Now try delete
DELETE FROM SmallTable WHERE Id = 4;

DELETE FROM SmallTable WHERE Id = 3;


-- Revert
DROP INDEX IX_BigChild_ParentId ON dbo.BigChild


SELECT * FROM SmallTable
DELETE FROM SmallTable WHERE Id = 2;






/* REVERT to normal
EXEC sys.sp_configure N'max server memory (MB)', N'6000'
RECONFIGURE WITH OVERRIDE
GO
EXEC sys.sp_configure N'show advanced options', N'0'  RECONFIGURE WITH OVERRIDE

DROP TABLE dbo.BigChild;
*/