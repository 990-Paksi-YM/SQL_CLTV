# SQL_CLTV

**BACKGROUND**

This project aims to provide insight to SuperStore's marketing team in order to use new marketing strategies that can increase SuperStore's sales growth. The analysis was conducted using the Customer Lifetime Value (CLV) metric.

Customer Lifetime Value (CLV) is a metric that calculates how much revenue is earned from customers while they are active customers by considering average value, purchase frequency, and customer lifetime.

**OVERVIEW**

CLV analysis is done by comparing LTV every year for 4 years. This is done to see consumers who have a growing CLTV value every year.

**ANALYZE**

There are a total of 20 columns in this dataset: 
Order_ID, Customer_ID,  Postal_Code, Product_ID, Sales, Quantity, Discount, Profit, Category, Sub_Category, Product_Name, Order_Date, Ship_Date, Ship_Mode, Customer_Name, Segment, Country, City, State, Region.

To calculate CLTV, we will use only a few columns:
Customer_ID,
Sales,
Order_Date,
Order_ID,
Sub_Category,
Product_Name,

Because the year date in this dataset is 4 years (2017, 2018, 2019, and 2020) and we want to find customer_id that has an increasing CLV value every year, then the CLTV calculation is carried out for each year separately and then combined at the end.

Note: Using Temporary Tables to speed up the query process.

**CONCLUSION & RECOMMENDATION**

From the data from the CLV query calculation results and getting a list of customer_ids that have increasing CLT values ​​every year, it can be a new insight for the marketing team to be able to target new consumer segments with sub_category or product_name recommendations from the customer_id list.
