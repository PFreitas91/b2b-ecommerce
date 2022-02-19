DECLARE @StartDate  date = '20100101';

DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, 30, @StartDate));

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
src AS
(
  SELECT
	sk_date = convert(int,convert(varchar,d,112)) ,
    TheDate         = CONVERT(date, d),
    TheDay          = DATEPART(DAY,       d),
    TheDayName      = DATENAME(WEEKDAY,   d),
    TheWeek         = DATEPART(WEEK,      d),
    TheISOWeek      = DATEPART(ISO_WEEK,  d),
    TheDayOfWeek    = DATEPART(WEEKDAY,   d),
    TheMonth        = DATEPART(MONTH,     d),
    TheMonthName    = DATENAME(MONTH,     d),
    TheQuarter      = DATEPART(Quarter,   d),
    TheYear         = DATEPART(YEAR,      d),
    TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    TheDayOfYear    = DATEPART(DAYOFYEAR, d)
  FROM d
)
SELECT *
into [TT_DW].dbo.dim_date 
FROM src
  ORDER BY TheDate
  OPTION (MAXRECURSION 0);


CREATE TABLE [TT_DW].dbo.dim_companies (
	sk_company int IDENTITY (1,1)  NOT NULL
	, CUIT int not null
	, CompanyName varchar(80)
	, CompanyType varchar(80)
 CONSTRAINT [PK_Dimension_Company] PRIMARY KEY CLUSTERED (sk_company)
 );


CREATE TABLE [TT_DW].dbo.[dim_customer](
	sk_customer int IDENTITY (1,1)  NOT NULL
	, sk_company int not null
	, CustomerID int not null
	, FullName varchar(80) not null
	, BirthDate date not null
	, Document varchar(80) not null
	, CUIT int
	, Country varchar(80)
	, username varchar(80)
 CONSTRAINT [PK_Dimension_Customer] PRIMARY KEY CLUSTERED (sk_customer)
 );
 



CREATE TABLE [TT_DW].dbo.dim_products (
	sk_product int IDENTITY (1,1)  NOT NULL
	, sk_suppliercompany int NOT NULL
	, ProductId int not null
	, ProductName varchar(250)
	, SupplierCUIT int not null
	, UnitPrice numeric(26,2)
	, Size varchar(80)
	, ValidFrom int not null
	, ValidTo int 
 CONSTRAINT [PK_Dimension_Product] PRIMARY KEY CLUSTERED (sk_product)
 );

CREATE TABLE [TT_DW].dbo.fact_orders (
	sk_orderline int IDENTITY (1,1)  NOT NULL
	, sk_product int not null
	, sk_customer int not null
	, OrderLineId int not null
	, OrderId int not null
	, ProductID int not null
	, Quantity int
	, UnitPrice numeric(26,2)
	, OrderDate int not null
	, CustomerID int not null
	, LastEditedWhen int 
	, OrderStatus varchar(80)
	, SupplierCUIT int
 CONSTRAINT [PK_Fact_Orders] PRIMARY KEY CLUSTERED (sk_orderline)
 , CONSTRAINT FK_Orders_Product FOREIGN KEY (sk_product)
     REFERENCES [TT_DW].dbo.dim_products (sk_product)
     ON DELETE CASCADE
     ON UPDATE CASCADE
 , CONSTRAINT FK_Orders_Customer FOREIGN KEY (sk_customer)
     REFERENCES [TT_DW].dbo.dim_customer (sk_customer)
     ON DELETE CASCADE
     ON UPDATE CASCADE
 );

create table [TT_DW].dbo.dim_marketinglead (
	sk_prospective_customer int IDENTITY (1,1)  NOT NULL
	,[FirstName] varchar(80)
    ,[LastName] varchar(80)
    ,[fullname] varchar(80)
    ,[Document] varchar(80)
    ,[BirthDate] date
    ,[campaign] varchar(80)
	,sk_customer int
	,converted_user bit 
	 CONSTRAINT [PK_Dimension_marketing_Customer] PRIMARY KEY CLUSTERED (sk_prospective_customer)
)

create table [TT_DW].dbo.fact_webvisits(
	sk_webvisit int IDENTITY (1,1)  NOT NULL,
	sk_customer int,
	remote_host varchar(80),
	username varchar(80),
	user_agent varchar(300),
	visit_date int,
	device varchar(80),
	no_visits int
 CONSTRAINT [PK_Fact_Visits] PRIMARY KEY CLUSTERED (sk_webvisit)
 , CONSTRAINT FK_Visits_Customer FOREIGN KEY (sk_customer)
     REFERENCES [TT_DW].dbo.dim_customer (sk_customer)
     ON DELETE CASCADE
     ON UPDATE CASCADE
 );

