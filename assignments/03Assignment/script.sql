show INDEX from sales;

alter table sales add index salesDate_salespersonid_non_clustered_composite_index(salespersonid, quantity, salesdate);
alter table sales add index salesDate_non_clustered_index(salesdate);
alter table products add index productID_to_price_non_clustered_composite_index (productid, price);
alter table products add primary key (productid);



select   
    sales_person_to_product_to_income_q.sales_person_id,
    sum(sales_person_to_product_to_income_q.money_per_category) as total_money 
from (select                                    -- using CTE here would not make a difference
          s.salespersonid as sales_person_id,
          p.productid as product_id, 
          sum(s.quantity)*p.price as money_per_category 
      from sales s
            inner join products p
            on p.productid = s.productid
            where s.salesdate > timestamp('2018-03-01') and s.salesdate < timestamp('2018-04-01')
            group by salespersonid, p.productid, p.price
    ) as sales_person_to_product_to_income_q
group by sales_person_to_product_to_income_q.sales_person_id
order by sales_person_to_product_to_income_q.sales_person_id;


-- optimised query 
with sales_person_to_product_to_income_q as (
    select                                    
        s.salespersonid as sales_person_id,
        p.productid as product_id,
        sum(s.quantity)*p.price as money_per_category
    from sales s
        inner join products p
        on p.productid = s.productid
    where s.salesdate > timestamp('2018-03-01') and s.salesdate < timestamp('2018-04-01')
    group by salespersonid, p.productid, p.price
)
select 
    sales_person_to_product_to_income_q.sales_person_id,
    sum(sales_person_to_product_to_income_q.money_per_category) as total_money
from sales_person_to_product_to_income_q 
group by sales_person_to_product_to_income_q.sales_person_id
order by sales_person_to_product_to_income_q.sales_person_id;







-- SUPER unoptimised query  
SELECT
    e.EmployeeID,
    (SELECT SUM(ipppsp.incomePerProductPerSalesPerson)  
     FROM (
              SELECT DISTINCT
                  s.SalesPersonID AS EmployeeID,
                  p.ProductID AS productID,
                  p.Price AS product_price,
                  SUM(s.quantity) OVER (PARTITION BY s.SalesPersonID, p.ProductID, p.Price) * p.Price AS incomePerProductPerSalesPerson
              FROM sales s
                       INNER JOIN products p ON p.ProductID = s.ProductID
          ) AS ipppsp
     WHERE ipppsp.EmployeeID = e.EmployeeID
    ) AS IncomePerEmployee,

    DATEDIFF(CURRENT_DATE, e.HireDate) AS total_working_days,

    (SELECT SUM(ipppsp.incomePerProductPerSalesPerson)
     FROM (
              SELECT DISTINCT
                  s.SalesPersonID AS EmployeeID,
                  p.ProductID AS productID,
                  p.Price AS product_price,
                  SUM(s.quantity) OVER (PARTITION BY s.SalesPersonID, p.ProductID, p.Price) * p.Price AS incomePerProductPerSalesPerson
              FROM sales s
                       INNER JOIN products p ON p.ProductID = s.ProductID
          ) AS ipppsp
     WHERE ipppsp.EmployeeID = e.EmployeeID
    ) / DATEDIFF(CURRENT_DATE, e.HireDate) AS earning_rate

FROM employees e;

alter table employees add primary key (employeeid);
alter table products add primary key (productid);
alter table categories add primary key (categoryid);
alter table employees add index hireDate_non_clustered_index (hiredate);
alter table sales add index salesPersonID_productID_non_clustered_index (salespersonid, productid);



-- optimised version
explain analyze with income_per_product_per_sales_person as (
    select distinct
        e.EmployeeID as EmployeeID,
        e.FirstName as FirstN,
        e.LastName as LastN,
        p.ProductID as productID,
        p.Price as product_price,
        sum(s.quantity) over (partition by e.EmployeeID, e.FirstName, e.LastName, p.ProductID, p.Price) as quantity,
        sum(s.quantity) over (partition by e.EmployeeID, e.FirstName, e.LastName, p.ProductID, p.Price) * p.Price as incomePerProductPerSalesPerson
    from sales s
             inner join employees e
                    on e.EmployeeID = s.SalesPersonID
             inner join products p
                    on p.ProductID = s.ProductID
             inner join categories c
                    on c.CategoryID = p.CategoryID
),
     income_per_employee as (
         select distinct
             ipppsp.EmployeeID,
             sum(ipppsp.incomePerProductPerSalesPerson) over (partition by ipppsp.EmployeeID) as IncomePerEmployee
         from income_per_product_per_sales_person ipppsp

     )
         select
             ipe.EmployeeID,
             ipe.IncomePerEmployee,
             datediff(CURRENT_DATE, e.HireDate) as total_working_days,
             ipe.IncomePerEmployee / datediff(CURRENT_DATE, e.HireDate) as earning_rate
         from income_per_employee ipe
             inner join employees e
             on e.EmployeeID = ipe.EmployeeID;


