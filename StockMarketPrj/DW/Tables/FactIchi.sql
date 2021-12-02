CREATE TABLE [DW].[FactIchi](
	[NmdID] [int] NOT NULL,
	[DteID] [int] NULL,
	[Tenkan-sen] [int] NULL,
	[Kijun-sen] [int] NULL,
	[Senkou Span A] [int] NULL,
	[Senkou Span B] [int] NULL,
	[Chinkou Span] [int] NULL,
	[ClosePrc] [decimal](26, 3) NULL
) ON [PRIMARY]
GO

