-- Insert new orders
insert into [TT_DW].dbo.fact_orders
select p.sk_product
	, c.sk_customer
	, ol.OrderLineId
	, o.OrderId
	, ol.ProductID
	, ol.Quantity
	, ol.UnitPrice
	, cast(convert(varchar,dateadd(yy,5,convert(date,o.OrderDate,103)),112) as int)  as OrderDate
	, o.CustomerID
	, case when convert(date,ol.LastEditedWhen,103) > convert(date,o.LastEditedWhen,103) 
		then cast(convert(varchar,dateadd(yy,5,convert(date,ol.LastEditedWhen,103)),112) as int) 
		else cast(convert(varchar,dateadd(yy,5,convert(date,o.LastEditedWhen,103)),112) as int) end as LastEditedWhen
	, 'Active' as OrderStatus
	, ol.SupplierCUIT
from [B2B_DB].dbo.Orders o
join [B2B_DB].dbo.Orderlines ol on o.OrderId = ol.OrderId
join [TT_DW].dbo.dim_products p on p.ProductId = ol.ProductID and p.SupplierCUIT = ol.SupplierCUIT
join [TT_DW].dbo.dim_customer c on c.CustomerID = o.CustomerID
left join [TT_DW].dbo.fact_orders dw on o.OrderId = dw.OrderId and ol.OrderLineId=dw.OrderLineId
where dw.OrderLineId is null

-- Inactive deleted orders
update [TT_DW].dbo.fact_orders
	set OrderStatus = 'Inactive'
where OrderId not in (select OrderId from [B2B_DB].dbo.Orders)

-- Updated orders
update f
	set f.Quantity = ol.Quantity
		,f.UnitPrice = ol.UnitPrice
		,f.LastEditedWhen = case when convert(date,ol.LastEditedWhen,103) > convert(date,o.LastEditedWhen,103) 
				then cast(convert(varchar,dateadd(yy,5,convert(date,ol.LastEditedWhen,103)),112) as int) 
				else cast(convert(varchar,dateadd(yy,5,convert(date,o.LastEditedWhen,103)),112) as int) end
from [TT_DW].dbo.fact_orders f
	join [B2B_DB].dbo.Orders o on f.OrderId = o.OrderId
	join [B2B_DB].dbo.Orderlines ol on o.OrderId = ol.OrderId
	join [TT_DW].dbo.dim_products p on p.ProductId = ol.ProductID and p.SupplierCUIT = ol.SupplierCUIT
	join [TT_DW].dbo.dim_customer c on c.CustomerID = o.CustomerID
where 
	cast(convert(varchar,dateadd(yy,5,convert(date,o.LastEditedWhen,103)),112) as int) <> f.LastEditedWhen