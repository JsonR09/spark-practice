# Customer Insights

## Description

The objective of this is to get the insights from the data. Datasets are available in the data directory. And the schema is as following :
- Product (product_id, product_name, product_type, product_version, product_price)
- Customer (customer_id, customer_first_name, customer_last_name, phone_number )
- Sales (transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity ) 
- Refund (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)

## Questions :
1. Write down the data quality issues with the datasets provided and steps performed to clean (if any).
2. Display the distribution of sales by product name and product type.
3. Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.
4. Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
5. Find a product that has not been sold at least once (if any).
6. Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.