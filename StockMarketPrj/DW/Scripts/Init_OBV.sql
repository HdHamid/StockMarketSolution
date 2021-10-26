CREATE Procedure [DW].[Init_OBV]
@Nmd int,@DteID int  
as 
set nocount on
--declare @Nmd int = 210
--declare @DteID int = 20181201


Declare @TbRslt table (RN int,NmdID int,DteID int,ClosePrc int,vol Decimal(38,0))

insert into @TbRslt (RN,NmdID,DteID,ClosePrc,vol)
select ROW_NUMBER() over(order by NmdID,DteID) RN,
NmdID,DteID,isnull(Lst_Prc,ClosePrc),Vol
from DW.FactNmds where NmdID = @Nmd and DteID >= @DteID


drop table if exists #ee
;with a as (select min(DteID) MinDte from DW.FactNmds where NmdID = @Nmd and  DteID >= @DteID) 
,Rcrsv as(
select t.*,Vol as OBV from @TbRslt t inner join a on t.NmdID = @Nmd and a.MinDte = t.DteID
Union ALL
select t.*,
case when t.ClosePrc = r.ClosePrc then r.OBV when t.ClosePrc > r.ClosePrc 
then r.OBV+t.vol when t.ClosePrc < r.ClosePrc then r.OBV-t.vol end 
from @TbRslt t inner join Rcrsv r on r.RN+1 = t.rn
)
select * into #ee from Rcrsv  option (maxrecursion 0)

delete DW.FactOBV where NmdID = @Nmd and DteID >= @DteID 
insert into DW.FactOBV(NmdID,DteID,OBV)
select NmdID,DteID,OBV from #ee

