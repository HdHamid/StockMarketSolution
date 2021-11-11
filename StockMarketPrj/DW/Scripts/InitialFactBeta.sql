create procedure [DW].[Init_FactBeta]
as

truncate table DW.FactBeta


drop table if exists #Kpis 
select f.NmdID,DteID,Fin_Prcnt
from dw.FactNmds f 
inner join dw.DimNmds d on d.NmdID = f.NmdID
where d.NmdDesc in ('KpiW','Kpi')


create clustered index IX on #Kpis (Dteid,nmdid)

declare @dtwid int 

select @dtwid = DteID from dw.GtRwsASDte where ID = 21

;with stp1(Nmdid , AvgX , AvgY) as (
select f.NmdID,Avg(f.Fin_Prcnt),Avg(k.Fin_Prcnt)
from dw.FactNmds f inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID where f.DteID > @dtwid
group by f.NmdID
)
,Stp2 as (
select s.Nmdid,30 as DtePrmtr
,(Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0) as Beta 
,avg((Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0)) over() as AvgBeta
from stp1 s 
inner join dw.FactNmds f on f.NmdID = s.Nmdid and f.DteID > @dtwid
inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID
inner join dw.DimNmds dn on dn.NmdID = f.NmdID and dn.Sts = 1
group by s.Nmdid
)
insert into DW.FactBeta(Nmdid,DtePrmtr,Beta,BetaRte,Aggr,AvgBeta)
select Nmdid,DtePrmtr,Beta,(Beta - AvgBeta)/AvgBeta as BetaRte,iif(Beta > AvgBeta , N'بالای میانگین' ,  N'زیر میانگین') Aggr 
,AvgBeta
from Stp2




select @dtwid = DteID from dw.GtRwsASDte where ID = 100



;with stp1(Nmdid , AvgX , AvgY) as (
select f.NmdID,Avg(f.Fin_Prcnt),Avg(k.Fin_Prcnt)
from dw.FactNmds f inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID where f.DteID > @dtwid
group by f.NmdID
)
,Stp2 as (
select s.Nmdid,180 as DtePrmtr
,(Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0) as Beta 
,avg((Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0)) over() as AvgBeta
from stp1 s 
inner join dw.FactNmds f on f.NmdID = s.Nmdid and f.DteID > @dtwid
inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID
inner join dw.DimNmds dn on dn.NmdID = f.NmdID and dn.Sts = 1
group by s.Nmdid
)
insert into DW.FactBeta(Nmdid,DtePrmtr,Beta,BetaRte,Aggr,AvgBeta)
select Nmdid,DtePrmtr,Beta,(Beta - AvgBeta)/AvgBeta as BetaRte,iif(Beta > AvgBeta , N'بالای میانگین' ,  N'زیر میانگین') Aggr 
,AvgBeta
from Stp2


select @dtwid = DteID from dw.GtRwsASDte where ID = 200


;with stp1(Nmdid , AvgX , AvgY) as (
select f.NmdID,Avg(f.Fin_Prcnt),Avg(k.Fin_Prcnt)
from dw.FactNmds f inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID where f.DteID > @dtwid
group by f.NmdID
)
,Stp2 as (
select s.Nmdid,365 as DtePrmtr
,(Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0) as Beta 
,avg((Sum((f.Fin_Prcnt - AvgX) * (k.Fin_Prcnt - AvgY))/nullif((COUNT(1)-1),0))/nullif(Var(k.Fin_Prcnt),0)) over() as AvgBeta
from stp1 s 
inner join dw.FactNmds f on f.NmdID = s.Nmdid and f.DteID > @dtwid
inner join #Kpis k on k.NmdID = 682 and k.DteID = f.DteID
inner join dw.DimNmds dn on dn.NmdID = f.NmdID and dn.Sts = 1
group by s.Nmdid
)
insert into DW.FactBeta(Nmdid,DtePrmtr,Beta,BetaRte,Aggr,AvgBeta)
select Nmdid,DtePrmtr,Beta,(Beta - AvgBeta)/AvgBeta as BetaRte,iif(Beta > AvgBeta , N'بالای میانگین' ,  N'زیر میانگین') Aggr 
,AvgBeta
from Stp2

 delete dw.FactBeta where beta is null 