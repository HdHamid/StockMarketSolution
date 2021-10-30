-- exec [DW].[Init_SMA_EMA] 200,210,20181201
-- select * from dw.FactNmds
CREATE procedure [DW].[Init_SMA_EMA]
@Days int 
,@NmdID int
,@DteID int
,@RSIAVGPeriod int = 9-- Rooye 5 RSI rooze
,@RSIDteID int = 0-- Rooye 5 RSI rooze

as
set nocount on
Declare @TbRslt table (RN int,NmdID int,DteID int,ClosePrc int,SMA decimal (12,2))

--declare @Days int = 9
--declare @NmdID int = 210
--declare @DteID int = 1

---SMA
declare @SMAQ nvarchar(Max) = N'
select 
ROW_NUMBER() over(PARTITION BY NmdID order by NmdID,DteID)RN,
NmdID,DteID,isnull(Lst_Prc,ClosePrc),
AVG(isnull(Lst_Prc,ClosePrc)) 
over(partition by NmdId order by NmdId,DteID ROWS '+cast((@Days-1) as nvarchar(3))+' PRECEDING) as SMA
from dw.FactNmds where nmdid = '+Cast(@NmdID as nvarchar(50))+' and DteID >= '+  Cast(@DteID as nvarchar(50))
insert into @TbRslt(RN,NmdID,DteID,ClosePrc,SMA)
exec(@SMAQ)

declare @P decimal(38,6)
set @p = (2 / (cast(@Days as decimal(38,6)) + 1))


drop table if exists #ee
;with a as (select NmdID,min(DteID) MinDte from @TbRslt group by NmdID) 
,Rcrsv as(
select t.*,SMA as EMA from @TbRslt t inner join a on a.NmdID = t.NmdID and a.MinDte = t.DteID
Union ALL
select t.*,cast((t.ClosePrc*@P) + r.EMA *(1-@p) as Decimal(12,2))
from @TbRslt t inner join Rcrsv r on t.RN = r.RN+1
)
select * into #ee from Rcrsv  option (maxrecursion 0)

delete DW.FactMAvg where NmdID = @NmdID AND DteID >= @DteID and [Period] = @Days
insert into DW.FactMAvg(NmdID,DteID,SMA,EMA,[Period])
select NmdID,DteID,SMA,EMA,@Days as [Period]  from #ee



--------------------------------------------> RSI5
--declare @NmdID int = 1
--declare @RSIAVGPeriod int = 9
--declare @RSIDteID int = 0

if( @RSIDteID <> 0 )
begin 
Declare @TbRsltRSI table (RN int,NmdID int,DteID int,RSI int,SMA decimal (12,2))

---SMA
declare @SMAQRSI nvarchar(Max) = N'
select 
ROW_NUMBER() over(PARTITION BY NmdID order by NmdID,DteID)RN,
NmdID,DteID,RSI,
AVG(RSI) 
over(partition by NmdId order by NmdId,DteID ROWS '+cast((@RSIAVGPeriod-1) as nvarchar(3))+' PRECEDING) as SMA
from dw.FactRSI where nmdid = '+Cast(@NmdID as nvarchar(50))+' and DteID >= '+  Cast(@RSIDteID as nvarchar(50)) 
+ ' and Period = 5 '
insert into @TbRsltRSI(RN,NmdID,DteID,RSI,SMA)
exec(@SMAQRSI)

declare @PRSI decimal(38,6)
set @PRSI = (2 / (cast(@RSIAVGPeriod as decimal(38,6)) + 1))


drop table if exists #eeRSI
;with a as (select NmdID,min(DteID) MinDte from @TbRsltRSI group by NmdID) 
,Rcrsv as(
select t.*,SMA as EMA from @TbRsltRSI t inner join a on a.NmdID = t.NmdID and a.MinDte = t.DteID
Union ALL
select t.*,cast((t.RSI*@PRSI) + r.EMA *(1-@PRSI) as Decimal(12,2))
from @TbRsltRSI t inner join Rcrsv r on t.RN = r.RN+1
)
select * into #eeRSI from Rcrsv  option (maxrecursion 0)

Update I set EMA9 = T1.EMA
from #eeRSI T1 inner join DW.FactRSI I on I.NmdID = T1.NmdID and I.DteID = T1.DteID and Period = 5
end 
