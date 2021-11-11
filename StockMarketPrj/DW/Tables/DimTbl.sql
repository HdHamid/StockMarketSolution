
CREATE TABLE [DW].[DimTbl](
	[TblID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	[TblNam] [nvarchar](50) NULL,
	[PrcntOsc] [decimal](4, 2) NULL,
	[PrntNam] [nvarchar](50) NULL
	) ON [PRIMARY]