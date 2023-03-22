from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('retail-insights').getOrCreate()


customers_schema = StructType([
    StructField('customer_id',       IntegerType(), nullable=True),
    StructField('customer_fname',    StringType(), nullable=True),
    StructField('customer_lname',    StringType(), nullable=True),
    StructField('customer_email',    StringType(), nullable=True),
    StructField('customer_password', StringType(), nullable=True),
    StructField('customer_street',   StringType(), nullable=True),
    StructField('customer_city',     StringType(), nullable=True),
    StructField('customer_state',    StringType(), nullable=True),
    StructField('customer_zipcode',  StringType(), nullable=True)])

customers_df = spark.read.schema(customers_schema).csv('Data\\Customers.csv')

products_schema = StructType([
    StructField('product_id',          IntegerType(), nullable=True),
    StructField('product_category_id', IntegerType(), nullable=True),
    StructField('product_name',        StringType(), nullable=True),
    StructField('product_description', StringType(), nullable=True),
    StructField('product_price',       FloatType(), nullable=True),
    StructField('product_image',       StringType(), nullable=True)])

products_df = spark.read.schema(products_schema).csv('Data\\Products.csv')

categories_schema = StructType([
    StructField('category_id',            IntegerType(), nullable=True),
    StructField('category_department_id', IntegerType(), nullable=True),
    StructField('category_name',          StringType(), nullable=True)])

categories_df = spark.read.schema(categories_schema).csv('Data\\Categories.csv')

orders_schema = StructType([
    StructField('order_id',          IntegerType(), nullable=True),
    StructField('order_date',        StringType(), nullable=True),
    StructField('order_customer_id', IntegerType(), nullable=True),
    StructField('order_status',      StringType(), nullable=True)])

orders_df = spark.read.schema(orders_schema).csv('Data\\Orders.csv')

departments_schema = StructType([
    StructField('department_id',   IntegerType(), nullable=True),
    StructField('department_name', StringType(), nullable=True)])

departments_df = spark.read.schema(departments_schema).csv('Data\\Departments.csv')

order_items_schema = StructType([
    StructField('order_item_id',            IntegerType(), nullable=True),
    StructField('order_item_order_id',      IntegerType(), nullable=True),
    StructField('order_item_product_id',    IntegerType(), nullable=True),
    StructField('order_item_quantity',      IntegerType(), nullable=True),
    StructField('order_item_subtotal',      FloatType(), nullable=True),
    StructField('order_item_product_price', FloatType(), nullable=True)])

orderItems_df = spark.read.schema(order_items_schema).csv('Data\\Order_items.csv')

#Find the total number of orders
print('Total number of orders : '+ str(orders_df.count()))

#Find the average revenue per order.
avgRevenuePerOrder_df = orders_df.join(orderItems_df, col('order_id') == col('order_item_order_id'))\
    .agg(round(sum('order_item_subtotal') / countDistinct('order_item_order_id'),2)\
                             .alias('average_revenue'))

#avgRevenuePerOrder_df.show()

#Find the average revenue per day.
avgRevenue_df = orders_df.join(orderItems_df, col('order_id') == col('order_item_order_id'))\
    .groupby('order_date').agg(round(sum('order_item_subtotal') / countDistinct('order_item_order_id'),2)\
                             .alias('average_revenue')).orderBy('order_date')

#Find the average revenue per month
avgRevenuePerMonth_df = orders_df.join(orderItems_df,col('order_id') == col('order_item_order_id'))\
    .groupby(month(col('order_date').cast(TimestampType())).alias('month')
             ,year(col('order_date').cast(TimestampType())).alias('year'))\
    .agg(round(avg('order_item_subtotal'),2).alias('average_revenue')).orderBy('year','month')

#avgRevenuePerMonth_df.show()

#Which departments have the best performance?

bestPerformingDepartment_df = categories_df\
    .join(departments_df,col('category_department_id') == col('department_id'))\
    .join(products_df, col('category_id') == col('product_category_id'))\
    .join(orderItems_df,col('product_id') == col('order_item_product_id'))\
    .join(orders_df, col('order_item_order_id') == col('order_id'))\
    .filter((col('order_status') != 'CANCELED') | (col('order_status') != 'SUSPECTED_FRAUD'))\
    .groupby('department_id','department_name').agg(sum('order_item_subtotal').alias('total_revenue'))\
    .orderBy(col('total_revenue').desc()).first()

print('Best performing department ever: '+bestPerformingDepartment_df[1])

#What is the most expensive item in the catalog?
mostExpensiveItem = products_df.agg(max('product_price')).collect()[0][0]
mostExpensiveItem_df = products_df.where(col('product_price') == mostExpensiveItem)

mostExpensiveItem_df.show()

#Which products have generated the most revenue?
productByRevenue_df = products_df.join(orderItems_df, col('product_id') == col('order_item_product_id'))\
    .join(orders_df, col('order_item_order_id') == col('order_id'))\
    .groupby('product_id','product_name').agg(round(sum('order_item_subtotal'),2).alias('total_revenue'))\
    .orderBy(col('total_revenue').desc())

#productByRevenue_df.show()

#What are the top-ordered categories in the retail data
topOrderedCategory_df = categories_df.join(products_df,col('category_id') == col('product_category_id'))\
    .join(orderItems_df,col('product_id') == col('order_item_product_id'))\
    .join(orders_df,col('order_item_order_id') == col('order_id'))\
    .groupby('category_id','category_name').agg(sum('order_item_quantity').alias('total_orders'))\
    .orderBy(col('total_orders').desc())

#topOrderedCategory_df.show()

topOrderedCategory_df_pd = topOrderedCategory_df.toPandas()
#labels=pdf['category_name'],
# topOrderedCategory_df_pd.plot(kind='pie', y = 'total_orders', autopct='%1.1f%%', startangle=90,
#                                legend=False, title='Most popular Categories',
#                               figsize=(9, 9))


#Find the count of orders based on their status.

ordersPerStatus_df = orders_df.groupby('order_status').agg(sum('order_id').alias('total_orders'))

#ordersPerStatus_df.show()

ordersPerStatus_df_pd = ordersPerStatus_df.toPandas()

plt.bar(ordersPerStatus_df_pd['order_status'],ordersPerStatus_df_pd['total_orders'])
plt.title('Number of orders by status')
plt.xlabel('Order Status')
plt.ylabel('Total Orders')
#plt.show()


#Find all orders whose status is CANCELED, and the order amount is more than $1000.

cancelledOrders_df = orders_df.filter(col('order_status') == 'CANCELED').join(orderItems_df, col('order_id') == col('order_item_order_id'))\
    .groupby('order_id').agg(round(sum('order_item_subtotal'),2).alias('total_revenue')).filter(col('total_revenue') > 1000)
#cancelledOrders_df.show()



# Find all customers who made more than five orders in August 2013.
customerFiveOrders_df = customers_df.join(orders_df, col('customer_id') == col('order_customer_id'))\
    .filter((month('order_date') == 8) & (year('order_date') == 2013))\
    .join(orderItems_df,col('order_id') == col('order_item_order_id')).groupby('customer_id','customer_fname','customer_lname')\
    .agg(count('order_id').alias('total_orders')).filter(col('total_orders') > 5).orderBy('total_orders',ascending=False)

customerFiveOrders_df.show()