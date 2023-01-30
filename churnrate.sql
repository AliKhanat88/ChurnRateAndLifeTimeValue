use northwind;
-- First way to calculate churn rate 
create table temp_table select total_customers,
		retained_customers,
        total_orders.year,
        total_orders.month,
        rank() over (order by year, month) as rank_of_months
from (select count(distinct CustomerID) as total_customers, month(OrderDate) as month, year(OrderDate) as year
from orders
group by year(OrderDate), month(OrderDate)) as total_orders
left join (select count(distinct o2.CustomerID) as retained_customers, year(o2.OrderDate) year, month(o2.OrderDate) month
from orders o1
left join orders o2
on o1.CustomerID = o2.CustomerID and 
month(o1.OrderDate) = month(date_add(o2.OrderDate, interval -1 month)) and
year(o1.OrderDate) = year(date_add(o2.OrderDate, interval -1 month))
group by year(o2.OrderDate), month(o2.OrderDate)) as retained_table
on total_orders.month = retained_table.month and total_orders.year = retained_table.year;

select t1.month, t1.year, (t1.total_customers - t2.retained_customers) / t1.total_customers * 100 as churned_customers 
from temp_table t1
join temp_table t2
on t2.rank_of_months = t1.rank_of_months + 1;

-- Second way of calculating churn

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
 		end as churned,
        case 
        when lag_diff = 1 then "Retained"
        else null
        end as retained
     from lag_lead_diff
)
select customer_order_year,
		customer_order_quarter,
        count(new_returned_retained) as new_returned_retained_customers,
        count(churned) as next_quar_churned,
		count(churned) / count(new_returned_retained) as next_quat_churn_rate,
        count(retained)
from churn_rate_table
group by 1, 2
order by 1, 2;