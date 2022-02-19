insert into [TT_DW].dbo.fact_webvisits
select 
	c.sk_customer
	, v.remote_host
	, c.username
	, v.request_header_user_agent as user_agent
	, convert(varchar,convert(date,left(v.time_received_utc_isoformat,10)),112) as visit_date
	, case when CHARINDEX(';',substring(request_header_user_agent,CHARINDEX('(', request_header_user_agent)+1,CHARINDEX(')', request_header_user_agent)-CHARINDEX('(', request_header_user_agent)-1))=0 
	then substring(request_header_user_agent,CHARINDEX('(', request_header_user_agent)+1,CHARINDEX(')', request_header_user_agent)-CHARINDEX('(', request_header_user_agent)-1)
	else substring(request_header_user_agent,CHARINDEX('(', request_header_user_agent)+1,CHARINDEX(';', request_header_user_agent)-CHARINDEX('(', request_header_user_agent)-1) end as device
	, count(1) as no_visits
from [B2B_DB].dbo.stg_weblogs v
	join [TT_DW].dbo.dim_customer c on c.username = v.remote_user
	left join [TT_DW].dbo.fact_webvisits dw on dw.visit_date = convert(varchar,convert(date,left(v.time_received_utc_isoformat,10)),112) and c.sk_customer = dw.sk_customer
where dw.sk_customer is null
group by c.sk_customer
	, v.remote_host
	, c.username
	, v.request_header_user_agent
	, left(v.time_received_utc_isoformat,10)