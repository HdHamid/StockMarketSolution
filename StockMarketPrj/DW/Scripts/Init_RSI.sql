CREATE Procedure [DW].[Init_RSI]
 @days int ,
 @Nmdid int ,
 @DteId int 
as
set nocount on
--declare @days int = 14 
--declare @Nmdid int = 36 
--declare @DteId int = 20180901 

--declare @TBL table (Rn int,NmdID int,DteID int,ClosePrc decimal(26,2),Vol int,Chng decimal(26,2))
drop table if exists #tbl
Create table #tbl (Rn int,NmdID int,DteID int,ClosePrc decimal(26,2),Vol decimal(26,0),Chng decimal(26,2))

insert into #tbl(rn,NmdID,DteID,ClosePrc,Vol,Chng)
select ROW_NUMBER()over(order by NmdID,DteID) Rn, NmdID,DteID,isnull(Lst_Prc,ClosePrc),Vol,isnull(Lst_Prc,ClosePrc) - LAG(isnull(Lst_Prc,ClosePrc)) over(Order by NmdID,DteID) Chngs
--sum(case when ClosePrc > 0 then ClosePrc else 0 end) over(order by NmdID,DteID Rows 14 Preceding)
from dw.FactNmds where NmdID = @Nmdid and DteID > = @DteId

drop table if exists #ee
create table #ee (Rn int,NmdID int,DteID int,ClosePrc decimal(26,6),Vol Decimal(26,0),Chng decimal(26,2),GainAvg decimal(26,2),LossAvg decimal(26,2))

declare @Q nvarchar(Max) = N';with a as (
select *, 
sum(case when Chng > 0 then Chng else 0 end) over(order by NmdID,DteID Rows '+cast(@days-1 as nvarchar(50))+' Preceding) / '+cast(@days as nvarchar(50))+'  as GainAvg,
sum(case when Chng < 0 then abs(Chng) else 0 end) over(order by NmdID,DteID Rows '+cast(@days-1 as nvarchar(50))+' Preceding) / '+cast(@days as nvarchar(50))+' as LossAvg 
from #tbl where rn <= '+cast(@days+1 as nvarchar(50))+'
Union all 
select t.* 
, (a.GainAvg * '+cast(@days-1 as nvarchar(50))+' + case when t.Chng > 0 then t.Chng else 0 end) / '+cast(@days as nvarchar(50))+'
, (a.LossAvg * '+cast(@days-1 as nvarchar(50))+' + case when t.Chng < 0 then abs(t.Chng) else 0 end) / '+cast(@days as nvarchar(50))+'
from #tbl t 
inner join a on t.Rn = a.Rn + 1 and t.Rn > '+cast(@days+1 as nvarchar(50))+')
select * from a option (maxrecursion 0)'
insert into #ee(Rn,NmdID,DteID,ClosePrc,Vol,Chng,GainAvg,LossAvg)
exec (@Q)

-- select * from #ee

Delete DW.FactRSI where NmdID = @Nmdid and DteID >= @DteID and [Period] = @days

;with Upd as (select *,case when LossAvg = 0 then NULL else GainAvg/LossAvg end as RS
,case when LossAvg = 0 then 100 else 100 - (100/(1+(GainAvg/LossAvg))) end RSI
from #ee) 
insert into  DW.FactRSI(NmdID,DteID,Chng,GainAvg,LossAvg,RS,RSI,[Period])
select NmdID,DteID,Chng,GainAvg,LossAvg,RS,RSI,@days as [Period] from upd

