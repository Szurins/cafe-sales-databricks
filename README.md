# Cafe sales data
## Sample raw data

|Transaction ID|Item|Quantity|Price Per Unit|Total Spent|Payment Method|Location|Transaction Date|
|--------------|----|--------|--------------|-----------|--------------|--------|----------------|
TXN_1961373|Coffee|2|2.0|4.0|Credit Card|Takeaway|2023-09-08
TXN_4977031|Cake|4|3.0|12.0|Cash|In-store|2023-05-16
TXN_4271903|Cookie|4|1.0|ERROR|Credit Card|In-store|2023-07-19
TXN_7034554|Salad|2|5.0|10.0|UNKNOWN|UNKNOWN|2023-04-27
TXN_3160411|Coffee|2|2.0|4.0|Digital Wallet|In-store|2023-06-11
TXN_2602893|Smoothie|5|4.0|20.0|Credit Card||2023-03-31
TXN_4433211|UNKNOWN|3|3.0|9.0|ERROR|Takeaway|2023-10-06

## Init
To initialize project run Init file inside a Databricks project to create a new catalog and corresponding schemas
```SQL
%sql
CREATE CATALOG IF NOT EXISTS `cafe-sales`;
CREATE SCHEMA IF NOT EXISTS `cafe-sales`.bronze;
CREATE SCHEMA IF NOT EXISTS `cafe-sales`.silver;
CREATE SCHEMA IF NOT EXISTS `cafe-sales`.gold;
```

## Bronze layer
This layer is responsible for loading data from csv file to Databricks' schemas
### Example code snippet
```Python
names_map = {"/Volumes/cafe-sales/default/cafe-sales/dirty_cafe_sales.csv": "caffee_sales"}
 for old_name, new_name in names_map.items():
    df = spark.read.format("csv").option("header", "true").load(old_name)    
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_').replace(',', '_'))

    df.write.mode("overwrite").saveAsTable(f"`cafe-sales`.bronze.{new_name}")
  

%sql
SELECT * FROM `cafe-sales`.bronze.caffee_sales
```
## Silver layer
This notebook is responsible for data cleaning (e.g. handling Uknown/Error values)
### Example code snippet
```Python
# Normalize error values handling ERROR/Unknown/null
df = df.withColumn("Payment_Method", 
                    fs.when(fs.col("Payment_Method").isNull() |
                    fs.col("Payment_Method").isin("ERROR", "UNKNOWN"),
                    "Unknown").otherwise(fs.col("Payment_Method")))
df.select("Payment_Method").distinct().display()
```

## Golden layer
This layer is responsible for preparing data for analytical tasks (e.g. showing best selling items in store)
### Example code snippet
```SQL
%sql
DROP TABLE IF EXISTS `cafe-sales`.gold.item_total_income;
-- Total sales income by item
CREATE TABLE `cafe-sales`.gold.item_total_income
AS
SELECT Item, SUM(Total_Spent) AS Total_Income
FROM `cafe-sales`.silver.cafe_sales_info
WHERE Item NOT LIKE 'Unknown'
GROUP BY Item
ORDER BY SUM(Total_Spent) DESC
```
