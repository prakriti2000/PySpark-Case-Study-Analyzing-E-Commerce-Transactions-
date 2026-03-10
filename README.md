# PySpark-Case-Study-Analyzing-E-Commerce-Transactions-

## Overview
This project analyzes a small dataset of e-commerce transactions using PySpark.  
The goal is to understand customer purchasing behavior, calculate total spending per customer, and identify which products were purchased.

The analysis is done using PySpark RDD transformations such as map, filter, reduceByKey, groupByKey, and join.

---

## Dataset

Each transaction contains the following fields:

- transaction_id: Unique ID for each transaction
- customer_id: Unique ID for each customer
- product_id: Unique ID for each product
- product_name: Name of the product
- category: Product category
- price: Price of the product
- quantity: Quantity purchased

Example data:

1,101,5001,Laptop,Electronics,1000.0,1  
2,102,5002,Headphones,Electronics,50.0,2  
3,101,5003,Book,Books,20.0,3  
4,103,5004,Laptop,Electronics,1000.0,1  
5,102,5005,Chair,Furniture,150.0,1  

---

## Technologies Used

- Python
- PySpark
- Apache Spark (local mode)

---

## PySpark Transformations Used

map()  
Converts each CSV string into structured data.

filter()  
Filters transactions where the quantity is greater than 1.

flatMap()  
Extracts product names from the dataset.

reduceByKey()  
Calculates total spending per customer.

groupByKey()  
Lists products purchased by each customer.

join()  
Combines transaction data with product category information.

collect()  
Retrieves the final results.

---

## Results

Total spending per customer

('102', 250.0)  
('101', 1060.0)  
('103', 1000.0)  

Customer 101 spent the most.

Products purchased per customer

('102', ['Headphones', 'Chair'])  
('101', ['Laptop', 'Book'])  
('103', ['Laptop'])  

Product and category information

('Laptop', (('101', 1000.0), 'Electronics'))  
('Laptop', (('103', 1000.0), 'Electronics'))  
('Headphones', (('102', 100.0), 'Electronics'))  
('Chair', (('102', 150.0), 'Furniture'))  
('Book', (('101', 60.0), 'Books'))  

---

## Conclusion

This project demonstrates how PySpark RDD transformations can be used to analyze transaction data.  
The analysis identified customer spending patterns, products purchased by each customer, and product category relationships.
