
CREATE TABLE [DW].[DimGrps](
	[GrpID] [int] NOT NULL PRIMARY KEY,
	[GrpNam] [nvarchar](255) NULL,
	[GrpDrtyNam] [nvarchar](255) NULL,
	[GrpCallNam] [nvarchar](255) NULL,
	[GrpCodStr] [varchar](4) NULL,
	[GrpNamCod] [nvarchar](1000) NULL,
	[Elected] [tinyint] NULL
) ON [PRIMARY]