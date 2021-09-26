CREATE TABLE [DW].[DimTbl] (
    [TblID]    INT            IDENTITY (1, 1) NOT NULL,
    [TblNam]   NVARCHAR (50)  NULL,
    [PrcntOsc] DECIMAL (4, 2) NULL,
    [PrntNam]  NVARCHAR (50)  NULL,
    CONSTRAINT [PK_DimTbl] PRIMARY KEY CLUSTERED ([TblID] ASC)
);

