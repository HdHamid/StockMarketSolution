
CREATE procedure [DW].[Fill_RegMxMin]
as

truncate table DW.RegMxMin


----------------------------------== MAX3
drop table if exists #TblMx

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 3 preceding and 3 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) --تعیین اینکه آیا رکورد جاری پیوت هست یا خیر 
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and --این قسمت برای نزولی بودن ترند هست که مثلث نشون بده ولی فعلا غیر فعالش میکنم که ترند عودی هم بشکنه خبر بده 
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx 
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,3
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.1 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0
		


----------------------------------== MAX5
drop table if exists #TblMx5

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 5 preceding and 5 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) -- شرط rn برای این بوده که مثلا ماکزیمم روز جاری که بالاتر از ترنده روش تاثیر نذاره
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and 
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx5 
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,5
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx5 tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.01 and 0.05 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0
		


----------------------------------== MAX8
drop table if exists #TblMx8

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 8 preceding and 8 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) -- شرط rn برای این بوده که مثلا ماکزیمم روز جاری که بالاتر از ترنده روش تاثیر نذاره
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx8
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,8
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx8 tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.01 and 0.05 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0
		
----------------------------------== MAX13
drop table if exists #TblMx13

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 13 preceding and 13 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) -- شرط rn برای این بوده که مثلا ماکزیمم روز جاری که بالاتر از ترنده روش تاثیر نذاره
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx13
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,13
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx13 tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.01 and 0.05 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0

----------------------------------== MAX21
drop table if exists #TblMx21

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 21 preceding and 21 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) -- شرط rn برای این بوده که مثلا ماکزیمم روز جاری که بالاتر از ترنده روش تاثیر نذاره
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx21
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,21
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx21 tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.01 and 0.05 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0





----------------------------------== MAX34
drop table if exists #TblMx34

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,max(ClosePrc) over(partition by nmdid order by dteid rows between 34 preceding and 34 following) as MXNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MXNo = ClosePrc and rn > 1, 1,0) as sts from stp2  ) -- شرط rn برای این بوده که مثلا ماکزیمم روز جاری که بالاتر از ترنده روش تاثیر نذاره
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MXNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where --MXNo < LGPrc and
					  dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
,iif(MXNo < LGPrc,0,1) as IsAsc
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
IsAsc,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /nullif(((n * (Sxx)) - (Sx * Sx)),0) AS a,

    ((n * Sxy) - (Sx * Sy))
   /nullif(((n * Sxx) - (Sx * Sx)),0) AS b,
    ((n * Sxy) - (Sx * Sy))
   /nullif(SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		),0) AS r
		 from reg89)


select *,a+b*today as predict into #TblMx34
from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))
and a+b*today is not null 

;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
IsAsc,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,1 as FlrTrndBrkTrnd 
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,34
,IsAsc,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMx34 tm on tm.NmdID = f.NmdID --and (f.Lst_Prc - tm.predict)/tm.predict between -0.01 and 0.05 --and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte
		--inner join dw.Regression rg on rg.NmdID = tm.NmdID and rg.a5 > 0

----------------------------------== MIN 3
drop table if exists #TblMn3

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 3 preceding and 3 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn3 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,3 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn3 tm on tm.NmdID = f.NmdID --and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.05 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)

--1 ترند نزولی
--0 کف قیمتی


--- select * from DW.RegMxMin where FlrTrndBrkTrnd = 1






----------------------------------== MIN 5
drop table if exists #TblMn5

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 5 preceding and 5 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn5 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,5 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn5 tm on tm.NmdID = f.NmdID --and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.05 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)





----------------------------------== MIN 8
drop table if exists #TblMn8

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 8 preceding and 8 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp5 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp5 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.05/5.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn8 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,8 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn8 tm on tm.NmdID = f.NmdID --and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.05 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)








----------------------------------== MIN 13
drop table if exists #TblMn13

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 13 preceding and 13 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp13 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp13 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.013/13.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn13 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,13 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn13 tm on tm.NmdID = f.NmdID -- and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.013 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)




----------------------------------== MIN 21
drop table if exists #TblMn21

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 21 preceding and 21 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp21 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp21 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.021/21.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn21 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,21 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn21 tm on tm.NmdID = f.NmdID -- and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.021 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)





----------------------------------== MIN 34
drop table if exists #TblMn34

;with stp1 as 
	(select ROW_NUMBER() over(partition by d.nmdid order by dteid desc) as rn,d.NmdID,d.NmdNam,DteID,ClosePrc			
			from dw.FactNmds f
			inner join dw.DimNmds d on d.NmdID = f.NmdID and d.Sts = 1
			where DteID > FORMAT(getdate() - 365 ,'yyyyMMdd'))
,stp2 as 
	(select *,min(ClosePrc) over(partition by nmdid order by dteid rows between 34 preceding and 34 following) as MnNo
			from stp1 where rn <= 89)
,stp3 as 
	(select *,iif(MnNo = ClosePrc and rn > 10, 1,0) as sts from stp2  )
,stp4 as (select *,ROW_NUMBER() over(partition by nmdid order by dteid desc) dtern from stp3 where sts = 1 and rn > 3)
,stp34 as (select * from stp4 where dtern < 3)
,stp6 as (select *,LAG(MnNo) over(partition by nmdnam order by dteid) LGPrc,LAG(DteID) over(partition by nmdnam order by dteid) LGDte from stp34 )
,stp7 as (select *,(select Endt from dw.DimDate where ID = LGDte) as LgDt,(select Endt from dw.DimDate where ID = DteID)  Dt
	from stp6 s where  MnNo > LGPrc and dtern = 1)
,stp8 as (select NmdID,NmdNam,ClosePrc,LGPrc,DteID,LGDte,1 as xStrt,cast(cast(Dt as datetime) as int)-cast(cast(LgDt as datetime) as int) as xEnd 
,cast(getdate() as int)- cast(cast(LgDt as datetime) as int) as today
from stp7)
,reg89 as (select NmdID,NmdNam,today,DteID,LGDte,
ClosePrc+LGPrc Sy,
xEnd*xEnd+1 Sxx,
xEnd+1  Sx,
LGPrc+ClosePrc*xEnd Sxy,
ClosePrc*ClosePrc+ LGPrc*LGPrc Syy,
2 n,
degrees(atn2((ClosePrc-LGPrc)/nullif((ClosePrc*0.034/34.1),0),xEnd)) as Degree
from stp8)

,abr89 as (SELECT *,
    ((Sy * Sxx) - (Sx * Sxy))
   /((n * (Sxx)) - (Sx * Sx)) AS a,

    ((n * Sxy) - (Sx * Sy))
   /((n * Sxx) - (Sx * Sx)) AS b,
    ((n * Sxy) - (Sx * Sy))
   /SQRT(
         (((n * Sxx) - (Sx * Sx))
         *((n * Syy - (Sy * Sy))))
		) AS r
		 from reg89)
select *,a+b*today as predict into #TblMn34 from abr89 where NmdID in (select NmdID from dw.FactNmds where DteID > format(GETDATE()-4,'yyyyMMdd'))


;with LstDte as (select NmdID,max(dteid)dteid from dw.FactNmds where DteID > format(getdate() - 10,'yyyyMMdd') group by NmdID)

insert into DW.RegMxMin
(
NmdID		   ,
NmdNam		   ,
Lst_Prc		   ,
predict		   ,
DteID		   ,
LGDte		   ,
FlrTrndBrkTrnd ,
Tbl,
a,
Pr_LGDte,
Pr_Dte,
Prmtr,
Degree
)
select f.NmdID,dn.NmdNam,f.Lst_Prc,tm.predict,tm.DteID,tm.LGDte
,0 as FlrTrndBrkTrnd
,isnull(dn.Tbl,N'احتمالا بورسی')as Tbl,tm.b as a,d.Frdt,d1.Frdt,34 Prmtr,tm.Degree
		from dw.FactNmds f 
		inner join LstDte l on l.NmdID = f.NmdID and l.dteid = f.DteID
		inner join dw.DimNmds dn on dn.NmdID = l.NmdID
		inner join #TblMn34 tm on tm.NmdID = f.NmdID --and  (f.Lst_Prc - tm.predict)/tm.predict between -0.03 and 0.034 and f.OpenPrc < f.Lst_Prc
		inner join dw.DimDate d on d.ID = tm.DteID
		inner join dw.DimDate d1 on d1.ID = tm.LGDte		
--where f.NmdID not in (select NmdID from DW.RegMxMin)


