CREATE TABLE [DW].[DimNmds] (
    [NmdID]        INT             IDENTITY (1, 1) NOT NULL,
    [NmdFullNam]   NVARCHAR (1500) NULL,
    [NmdCod]       NVARCHAR (150)  NULL,
    [GrpID]        INT             NULL,
    [TblID]        INT             NULL,
    [BseVol]       DECIMAL (26)    NULL,
    [NmdCod2]      NVARCHAR (150)  NULL,
    [Sts]          TINYINT         CONSTRAINT [DF_DimNmds_Sts] DEFAULT ((1)) NULL,
    [NmdNamCod]    NVARCHAR (200)  NULL,
    [Tbl]          NVARCHAR (200)  NULL,
    [NmdNam]       AS              (replace(replace([nmdnamCod],N'ي',N'ی'),N'ك',N'ک')) PERSISTED,
    [NmdUrlID]     NVARCHAR (50)   NULL,
    [PrcntOsc]     DECIMAL (4, 2)  NULL,
    [StckCnt]      DECIMAL (36)    NULL,
    [FloatPrcnt]   DECIMAL (5, 2)  NULL,
    [FloatStckCnt] DECIMAL (28, 2) NULL,
    [Expr]         BIT             NULL,
    [SubSet]       BIT             NULL,
    CONSTRAINT [PK_DimNmds] PRIMARY KEY CLUSTERED ([NmdID] ASC),
    CONSTRAINT [FK_DimNmds_DimGrps] FOREIGN KEY ([GrpID]) REFERENCES [DW].[DimGrps] ([GrpID]),
    CONSTRAINT [FK_DimNmds_DimTbl] FOREIGN KEY ([TblID]) REFERENCES [DW].[DimTbl] ([TblID])
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'تعداد سهام', @level0type = N'SCHEMA', @level0name = N'DW', @level1type = N'TABLE', @level1name = N'DimNmds', @level2type = N'COLUMN', @level2name = N'StckCnt';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'درصد شناوری', @level0type = N'SCHEMA', @level0name = N'DW', @level1type = N'TABLE', @level1name = N'DimNmds', @level2type = N'COLUMN', @level2name = N'FloatPrcnt';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'تعداد سهام شناور', @level0type = N'SCHEMA', @level0name = N'DW', @level1type = N'TABLE', @level1name = N'DimNmds', @level2type = N'COLUMN', @level2name = N'FloatStckCnt';

