-- insert new rows
insert into [TT_DW].dbo.dim_marketinglead
select stg.[FirstName]
      ,stg.[LastName]
      ,stg.[fullname]
      ,stg.[Document]
      ,stg.[BirthDate]
	  , stg.campaign
	  , c.sk_customer
	  , case when c.sk_customer is null then 0 else 1 end as  converted_user
  FROM [B2B_DB].[dbo].[stg_marketinglead] stg
	left join [TT_DW].dbo.dim_customer c 
		on c.fullname = stg.fullname 
		and c.document = stg.document 
		and c.[BirthDate] = stg.[BirthDate]
	left join [TT_DW].dbo.dim_marketinglead dw 
		on dw.fullname = stg.fullname 
		and dw.document = stg.document 
		and dw.[BirthDate] = stg.[BirthDate]
where dw.fullname is null

--update new converted customers
update dw
set dw.sk_customer = c.sk_customer
	,dw.converted_user = 1
from [TT_DW].dbo.dim_marketinglead dw
	join [TT_DW].dbo.dim_customer c 
		on c.fullname = dw.fullname 
		and c.document = dw.document 
		and c.[BirthDate] = dw.[BirthDate]
where dw.sk_customer is null
