-- exec DW.Ichimoku 210
CREATE Procedure [DW].[InitIchi]
@NmdID int 
as
set nocount on
/*
Tenkan-sen: (highest high + lowest low)/2 calculated over last 9 periods;
Kijun-sen: (highest high + lowest low)/2 calculated over last 26 periods.
Senkou Span A: (Tenkan line + Kijun Line)/2 plotted 26 time periods ahead;
Senkou Span B: (highest high + lowest low)/2 calculated over past 52 time periods, sent 26 periods ahead.
Chinkou Span: (most current closing price plotted 26 time periods back
*/

--declare @NmdID int = 210 

delete  DW.FactIchi where nmdid = @NmdID

; With Ichi1 as (select DteID,isnull(Lst_Prc,ClosePrc) as ClosePrc
,Max(MaxPrc) over(Order by dteID rows 8 preceding)Hihgest9,MIN(minPrc) over(Order by dteID rows 8 preceding)Lowest9
,Max(MaxPrc) over(Order by dteID rows 25 preceding)Hihgest26,MIN(minPrc) over(Order by dteID rows 25 preceding)Lowest26
,Max(MaxPrc) over(Order by dteID rows 51 preceding)Hihgest52,MIN(minPrc) over(Order by dteID rows 51 preceding)Lowest52
from DW.FactNmds where NmdID = @NmdID)
,Ichi2 as (select *
,(Hihgest9+Lowest9)/2 as [Tenkan-sen]
,(Hihgest26+Lowest26)/2 as [Kijun-sen]
from Ichi1)
insert into  DW.FactIchi(NmdID,DteID,ClosePrc,[Tenkan-sen],[Kijun-sen],[Senkou Span A],[Senkou Span B],[Chinkou Span])
select top 52 @NmdID as NmdID,DteID,ClosePrc,[Tenkan-sen],[Kijun-sen]
,([Tenkan-sen]+[Kijun-sen]) / 2 as [Senkou Span A] 
,(Hihgest52+Lowest52) / 2 as [Senkou Span B]
,LEAD(ClosePrc,26) over(order by DteID) [Chinkou Span] 
from Ichi2
order by DteID desc 

