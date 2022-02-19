insert into [TT_DW].dbo.dim_companies
select 	a.CUIT
	, a.CompanyName
	, a.CompanyType
from [B2B_DB].dbo.Companies a
left join [TT_DW].dbo.dim_companies b on a.CUIT = b.CUIT
where b.CUIT is null;