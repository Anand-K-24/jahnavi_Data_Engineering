# Databricks notebook source
from pyspark.sql import Row

data1 = [Row(id=i, name=f"Name_{i}") for i in range(1, 21)]
df1 = spark.createDataFrame(data1)
df1.show()
data2 = [Row(id=i, city=f"City_{i}") for i in range(10, 30)]
df2 = spark.createDataFrame(data2)
df2.show()
df = df1.createOrReplaceTempView("customer")
df3= df2.createOrReplaceTempView("data1")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer
# MAGIC where not exists(select data1.id from data1 where data1.id = customer.id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Sales table
# MAGIC
# MAGIC CREATE TABLE Sales (
# MAGIC     sale_id INT PRIMARY KEY,
# MAGIC     product_id INT,
# MAGIC     quantity_sold INT,
# MAGIC     sale_date DATE,
# MAGIC     total_price DECIMAL(10, 2)
# MAGIC     FOREIGN KEY (product_id) REFERENCES Products(product_id)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data into Sales table
# MAGIC
# MAGIC INSERT INTO Sales (sale_id, product_id, quantity_sold, sale_date, total_price) VALUES
# MAGIC (1, 101, 5, '2024-01-01', 2500.00),
# MAGIC (2, 102, 3, '2024-01-02', 900.00),
# MAGIC (3, 103, 2, '2024-01-02', 60.00),
# MAGIC (4, 104, 4, '2024-01-03', 80.00),
# MAGIC (5, 105, 6, '2024-01-03', 90.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create Products table
# MAGIC
# MAGIC CREATE TABLE Products (
# MAGIC     product_id INT PRIMARY KEY,
# MAGIC     product_name VARCHAR(100),
# MAGIC     category VARCHAR(50),
# MAGIC     unit_price DECIMAL(10, 2)
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data into Products table
# MAGIC
# MAGIC INSERT INTO Products (product_id, product_name, category, unit_price) VALUES
# MAGIC (101, 'Laptop', 'Electronics', 500.00),
# MAGIC (102, 'Smartphone', 'Electronics', 300.00),
# MAGIC (103, 'Headphones', 'Electronics', 30.00),
# MAGIC (104, 'Keyboard', 'Electronics', 20.00),
# MAGIC (105, 'Mouse', 'Electronics', 15.00);
