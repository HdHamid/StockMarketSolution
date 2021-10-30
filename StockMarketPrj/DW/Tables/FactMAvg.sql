﻿CREATE TABLE [DW].[FactMAvg](
	[NmdID] [int] NULL,
	[DteID] [int] NULL,
	[SMA] [decimal](12, 2) NULL,
	[EMA] [decimal](12, 2) NULL,
	[Period] [int] NULL
) ON [PRIMARY]
GO

ALTER TABLE [DW].[FactMAvg]  WITH CHECK ADD  CONSTRAINT [FK_FactMAvg_DimDate] FOREIGN KEY([DteID])
REFERENCES [DW].[DimDate] ([ID])
GO

ALTER TABLE [DW].[FactMAvg] CHECK CONSTRAINT [FK_FactMAvg_DimDate]
GO

ALTER TABLE [DW].[FactMAvg]  WITH CHECK ADD  CONSTRAINT [FK_FactMAvg_DimNmds] FOREIGN KEY([NmdID])
REFERENCES [DW].[DimNmds] ([NmdID])
GO

ALTER TABLE [DW].[FactMAvg] CHECK CONSTRAINT [FK_FactMAvg_DimNmds]
GO

