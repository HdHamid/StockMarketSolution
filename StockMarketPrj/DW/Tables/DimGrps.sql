CREATE TABLE [DW].[DimGrps] (
    [GrpID]      INT             NOT NULL,
    [GrpNam]     NVARCHAR (255)  NULL,
    [GrpDrtyNam] NVARCHAR (255)  NULL,
    [GrpCallNam] NVARCHAR (255)  NULL,
    [GrpCodStr]  VARCHAR (4)     NULL,
    [GrpNamCod]  NVARCHAR (1000) NULL,
    [Elected]    TINYINT         NULL,
    CONSTRAINT [PK_DimGrps] PRIMARY KEY CLUSTERED ([GrpID] ASC)
);

