--Calculate total sales amount per customer.
select sales_customer_id, sum(sales_price * sales_quantity) price_x_qty 
from final_table
group by sales_customer_id;

--Determine the average order quantity per product.
select sales_product_id, round(avg(sales_quantity),1) avg_qty from final_table group by sales_product_id;

--Identify the top-selling products.
select sales_product_id, sum(sales_price * sales_quantity) price_x_qty from final_table group by sales_product_id order by price_x_qty desc;

--Identify top customers.
select users_name, sum(sales_price * sales_quantity) price_x_qty
from final_table
group by users_name
order by price_x_qty desc;

--Yearly sales trends
select strftime('%Y', sales_order_date) AS year
, sum(sales_price * sales_quantity) price_x_qty
, count(distinct sales_customer_id) unique_number_of_customers
, count(sales_customer_id) number_of_customers
, count(sales_order_id) number_of_orders
, sum(sales_quantity) number_of_items
, sum(sales_quantity) / count(sales_order_id) avg_basket_size
from final_table
group by strftime('%Y', sales_order_date);

--Quarterly sales trends
select strftime('%Y', sales_order_date) || '-Q' || (((strftime('%m', sales_order_date) - 1) / 3) + 1) AS quarter
, sum(sales_price * sales_quantity) price_x_qty
, count(distinct sales_customer_id) unique_number_of_customers
, count(sales_customer_id) number_of_customers
, count(sales_order_id) number_of_orders
, sum(sales_quantity) number_of_items
, sum(sales_quantity) / count(sales_order_id) avg_basket_size
from final_table
group by strftime('%Y', sales_order_date) || '-Q' || (((strftime('%m', sales_order_date) - 1) / 3) + 1);

--Monthly sales trends
select strftime('%Y', sales_order_date) || '-' || strftime('%m', sales_order_date) AS month_year
, sum(sales_price * sales_quantity) price_x_qty
, count(distinct sales_customer_id) unique_number_of_customers
, count(sales_customer_id) number_of_customers
, count(sales_order_id) number_of_orders
, sum(sales_quantity) number_of_items
, sum(sales_quantity) / count(sales_order_id) avg_basket_size
from final_table
group by strftime('%Y', sales_order_date) || '-' || strftime('%m', sales_order_date);


--Weather Sales Trend
select strftime('%Y', sales_order_date) || '-' || strftime('%m', sales_order_date) AS month_year
, weather_description 
, sum(sales_price * sales_quantity) price_x_qty
, count(distinct sales_customer_id) unique_number_of_customers
, count(sales_customer_id) number_of_customers
, count(sales_order_id) number_of_orders
, sum(sales_quantity) number_of_items
, sum(sales_quantity) / count(sales_order_id) avg_basket_size
from final_table
group by weather_description
, strftime('%Y', sales_order_date) || '-' || strftime('%m', sales_order_date)
order by strftime('%Y', sales_order_date) || '-' || strftime('%m', sales_order_date);

--Product Sales with respect to weather_description
select weather_description 
, sales_product_id
, sum(sales_price * sales_quantity) price_x_qty
, count(distinct sales_customer_id) unique_number_of_customers
, count(sales_customer_id) number_of_customers
, count(sales_order_id) number_of_orders
, sum(sales_quantity) number_of_items
, sum(sales_quantity) / count(sales_order_id) avg_basket_size
from final_table
group by weather_description 
, sales_product_id
order by price_x_qty desc;