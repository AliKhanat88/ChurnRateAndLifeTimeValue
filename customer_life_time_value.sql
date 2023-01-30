with quarterly_customers as (
	select CustomerID,
			quarter(orderDate) customer_order_quarter,
            year(OrderDate) customer_order_year,
			timestampdiff(quarter, "1970-01-01", OrderDate) time_period
	from orders
    group by 1, 2 
    order by 1, 2
),
lag_lead_table as (
	select *,
			lag(time_period) over (partition by CustomerID order by time_period) as lag_quar,
            lead(time_period) over (partition by CustomerID order by time_period) as lead_quar
    from quarterly_customers
),
lag_lead_diff as (
	select *, 
		time_period - lag_quar as lag_diff,
    	lead_quar - time_period as lead_diff
    from lag_lead_table
    
),
churn_rate_table as (
	select *,
 		case
 		when isnull(lag_diff) then "New"
        when lag_diff > 1 then "Returned"
        when lag_diff = 1 then "Retained"
        else null
        end as new_returned_retained,
        case
 		when isnull(lead_diff) or lead_diff > 1 then "Churned"
 		end as churned
     from lag_lead_diff
),
next_quat_churned as (
select customer_order_year,
		customer_order_quarter,
        count(new_returned_retained) as new_returned_retained_customers,
        count(churned) as next_quat_churned,
		count(churned) / count(new_returned_retained) as next_quat_churn_rate
from churn_rate_table
group by 1, 2
order by 1, 2),
this_quat_churned as (
select customer_order_year,
	customer_order_quarter,
    new_returned_retained_customers,
	lag(next_quat_churn_rate) over (order by customer_order_year, customer_order_quarter) as this_month_churn_rate,
    lag(next_quat_churned) over (order by customer_order_year, customer_order_quarter) as this_month_churned
from next_quat_churned)
select qc.customer_order_year,
	qc.customer_order_quarter,
    qc.this_month_churned,
    qc.this_month_churn_rate,
    count(distinct CustomerID) as customers_in_month,
    sum(od.Quantity * od.UnitPrice) / count(distinct CustomerID) as avg_revenue_by_cust,
    -- lets say we got 30% profit from every order customer in a quarter
    (sum(od.Quantity * od.UnitPrice) / count(distinct CustomerID) * .30) / this_month_churn_rate as CLV
from this_quat_churned qc
join orders o
on customer_order_year = year(OrderDate) and customer_order_quarter = quarter(OrderDate)
join `order details` od
on o.OrderID = od.OrderID
group by 1, 2