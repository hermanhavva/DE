select * from assignment1_x_comp.countries ;

select * from assignment1_x_comp.categories;

select * from assignment1_x_comp.cities ;

select * from assignment1_x_comp.customers ;

select * from assignment1_x_comp.employees ;

select * from assignment1_x_comp.sales; 

select * from assignment1_x_comp.products p;


--select distinct sales.productID from assignment1_x_comp.sales sales 
--inner join assignment1_x_comp.products products on 
--products.ProductID = sales.ProductID 
--inner join assignment1_x_comp.employees employees on
--employees.EmployeeID = sales.SalesPersonID;
--
--select distinct s.SalesPersonID, e.FirstName, e.MiddleInitial FROM assignment1_x_comp.sales s 
--inner join assignment1_x_comp.employees e on
--e.EmployeeID = s.SalesPersonID; 
--
--
--with (select e.EmployeeID, e.FirstName, e.LastName, c.CategoryID, sum(s.quantity) from assignment1_x_comp.sales s 
--	
--	inner join assignment1_x_comp.employees e 
--	on e.EmployeeID = s.SalesPersonID 
--	inner join assignment1_x_comp.products p 
--	on p.ProductID = s.ProductID 
--	inner join assignment1_x_comp.categories c
--	on c.CategoryID = p.CategoryID 
--	group by e.EmployeeID, e.FirstName, e.LastName, C.CategoryID
--	order by e.EmployeeID asc, sum(s.quantity) desc;
--	
--	)
	
	
	-- income per employee per specific product
with income_per_product_per_sales_person as (
select distinct
	e.EmployeeID as EmployeeID, 
	e.FirstName as FirstN, 
	e.LastName as LastN, 
	p.ProductID as productID, 
	p.Price as product_price, 
	sum(s.quantity) over (partition by e.EmployeeID, e.FirstName, e.LastName, p.ProductID, p.Price) as quantity, 
	sum(s.quantity) over (partition by e.EmployeeID, e.FirstName, e.LastName, p.ProductID, p.Price) * p.Price as incomePerProductPerSalesPerson 
from assignment1_x_comp.sales s 
	inner join assignment1_x_comp.employees e 
		on e.EmployeeID = s.SalesPersonID 
	inner join assignment1_x_comp.products p 
		on p.ProductID = s.ProductID 
	inner join assignment1_x_comp.categories c
		on c.CategoryID = p.CategoryID 
--	group by e.EmployeeID, e.FirstName, e.LastName, p.ProductID, p.Price
--	order by e.EmployeeID asc, sum(s.quantity) desc
	),  
	
income_per_employee as (
select distinct
	ipppsp.EmployeeID, 
	sum(ipppsp.incomePerProductPerSalesPerson) over (partition by ipppsp.EmployeeID) as IncomePerEmployee
from income_per_product_per_sales_person ipppsp 
	
),

average_income as (
select
	avg(ipe.IncomePerEmployee) as AvgIncomeAcrossEmployees 
from income_per_employee ipe
),

best_performing_sales_persons as (
select 
    ipe.EmployeeID, 
    ipe.IncomePerEmployee,
    average_income.AvgIncomeAcrossEmployees as avg_income
from income_per_employee ipe
	cross join average_income 
	where ipe.IncomePerEmployee > average_income.AvgIncomeAcrossEmployees
),

employees_to_earning_rate as (
select
	ipe.EmployeeID,
	ipe.IncomePerEmployee,
	datediff('day', CAST(e.HireDate as DATE), CURRENT_DATE) as total_working_days, 
	ipe.IncomePerEmployee / datediff('day', CAST(e.HireDate as DATE), CURRENT_DATE) as earning_rate
from income_per_employee ipe 
	inner join assignment1_x_comp.employees e 
	on e.EmployeeID = ipe.EmployeeID
)

--  BEST PERFORMING EMPLOYEES BASED ON INCOME RATE (money per day) 
--select * from employees_to_earning_rate eter
--order by eter.earning_rate desc
--limit 10;

-- BEST PERFORMING EMPLOYEES BASED ON TOTAL INCOME
--select * from best_performing_sales_persons bpsp
--order by bpsp.IncomePerEmployee;






--select e.HireDate, datediff('day', CAST(e.HireDate as DATE), CURRENT_DATE) from assignment1_x_comp.employees e ;
--
--
--
--
--select ipppsp.EmployeeID, sum(ipppsp.incomePerProductPerSalesPerson) from income_per_product_per_sales_person ipppsp 
--group by ipppsp.EmployeeID;
--	
--
--
--select distinct 
--	sales.ProductID, 
--	categories.CategoryName,
--	employees.FirstName, 
--	employees.LastName,
--
--	rank() over (partition by categories.CategoryID, employees.EmployeeID order by sum(sales.Quantity) group by  )
--	from assignment1_x_comp.sales sales  
--
--	inner join assignment1_x_comp.products products on 
--	products.ProductID = sales.ProductID 
--
--	inner join assignment1_x_comp.categories categories on 
--	categories.CategoryID = products.CategoryID 
--	
--	inner join assignment1_x_comp.employees employees on
--	employees.EmployeeID = sales.SalesPersonID
--	
--	;

	-- 	order by products.ProductID ;



