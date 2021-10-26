
CREATE TABLE [DW].[FactRSI](
	[NmdID] [int] NULL,
	[DteID] [int] NULL,
	[Chng] [decimal](26, 2) NULL,
	[GainAvg] [decimal](26, 2) NULL,
	[LossAvg] [decimal](26, 2) NULL,
	[RS] [decimal](38, 12) NULL,
	[RSI] [decimal](38, 23) NULL,
	[Period] [int] NOT NULL,
	[EMA9] [decimal](12, 2) SPARSE  NULL
) ON [PRIMARY]
GO

ALTER TABLE [DW].[FactRSI]  WITH CHECK ADD  CONSTRAINT [FK_FactRSI_DimDate] FOREIGN KEY([DteID])
REFERENCES [DW].[DimDate] ([ID])
GO

ALTER TABLE [DW].[FactRSI] CHECK CONSTRAINT [FK_FactRSI_DimDate]
GO

ALTER TABLE [DW].[FactRSI]  WITH CHECK ADD  CONSTRAINT [FK_FactRSI_DimNmds] FOREIGN KEY([NmdID])
REFERENCES [DW].[DimNmds] ([NmdID])
GO

ALTER TABLE [DW].[FactRSI] CHECK CONSTRAINT [FK_FactRSI_DimNmds]
GO


