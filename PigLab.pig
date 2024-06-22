
--Loading data
data = LOAD 'customer_purchases.csv' USING PigStorage(',') AS (customer_id:int, purchase_date:chararray, product_id:int, product_name:chararray, quantity:int, total_amount:double);

DUMP data;

--Calculate Total Purchase Amount per Customer:

grouped_data = GROUP data BY customer_id;

--DUMP grouped_data;

sum_data = FOREACH grouped_data GENERATE group AS customer_id, SUM(data.total_amount) as total_purchase_amount;

--DUMP sum_data;

--Find the Most Purchased Product:
grouped_prod = GROUP data by product_id;

product_total = FOREACH grouped_prod GENERATE group  AS product_id, SUM(data.quantity) as total_quant;

ordered_total = ORDER product_total by total_quant DESC;

most_purchased_product = LIMIT ordered_total 1;

--DUMP top_product;

--Calculate Average Purchase Amount per Customer:
grouped_data = GROUP data by customer_id;

avg_data = FOREACH grouped_data GENERATE group AS customer_id, AVG(data.total_amount) as avgerage_purchase_amount;

--DUMP avg_data;

--Identify High-Value Customers:
sum_data = FOREACH grouped_data GENERATE group AS customer_id, SUM(data.total_amount) as total_amount;

high_value_customer = FILTER sum_data by total_amount > 100.0;

--Sort Customers by Purchase Frequency:
purchase_count = FOREACH grouped_data GENERATE group AS customer_id, COUNT(data) as total_purchases;

ordered_customers = ORDER purchase_count by total_purchases DESC;

-- Generate a Report:
grouped_data = GROUP data BY customer_id;

-- Calculate total and average purchase amount for each customer
customer_totals = FOREACH grouped_data GENERATE 
group AS customer_id, 
SUM(data.total_amount) AS total_purchase_amount, 
AVG(data.total_amount) AS average_purchase_amount;
	
customer_product_group = GROUP data BY (customer_id,product_id);

product_counts = FOREACH customer_product_group GENERATE FLATTEN(group)  AS (customer_id,product_id), SUM(data.quantity) as total_quantity;

customer_group = GROUP product_counts BY customer_id;

top_product = FOREACH customer_group GENERATE group as customer_id, FLATTEN(product_counts.(product_id,total_quantity)) AS (product_id, total_quantity), MAX(product_counts.total_quantity) as most_purchased_product;

result = FILTER top_product by total_quantity == most_purchased_product;

filtered_top_product = FOREACH result GENERATE customer_id, product_id, total_quantity;

-- Join customer_totals and customer_products on customer_id
joined_data = JOIN customer_totals BY customer_id, filtered_top_product BY customer_id;

-- Add high-value customer flag
customer_report = FOREACH joined_data GENERATE 
customer_totals::customer_id, 
customer_totals::total_purchase_amount, 
customer_totals::average_purchase_amount, 
filtered_top_product::product_id AS most_purchased_product, 
(customer_totals::total_purchase_amount > 700.0 ? 'yes' : 'no') AS high_value_customer;

-- Store the final report
STORE customer_report INTO 'customer_report' USING PigStorage(',');