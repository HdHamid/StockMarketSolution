CREATE TABLE [DW].[RegMxMin](
	[NmdID] [int] NOT NULL,
	[NmdNam] [nvarchar](4000) NULL,
	[Lst_Prc] [decimal](26, 2) NULL,
	[predict] [decimal](38, 6) NULL,
	[DteID] [int] NULL,
	[LGDte] [int] NULL,
	[FlrTrndBrkTrnd] [int] NOT NULL,
	[Tbl] [nvarchar](100) NULL,
	[a] [decimal](38, 4) NULL,
	[Pr_Dte] [varchar](10) NULL,
	[Pr_LGDte] [varchar](10) NULL,
	[Prmtr] [smallint] NULL,
	[IsAsc] [bit] NULL,
	[Degree] [int] NULL
) ON [PRIMARY]