
CREATE TABLE [DW].[FactBeta](
	[Nmdid] [int] NOT NULL,
	[DtePrmtr] [int] NULL,
	[Beta] [float] NULL,
	[BetaRte] [decimal](10, 2) NULL,
	[Aggr] [nvarchar](100) NULL,
	[AvgBeta] [decimal](36, 2) NULL
) ON [PRIMARY]