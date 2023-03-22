from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, countDistinct, sum, dense_rank, count, max, row_number, when, rank
import pandas as pd

spark = SparkSession.builder.appName("restaurant-insights").getOrCreate()

sales_data = [
          ('A', '2021-01-01', '1'),
          ('A', '2021-01-01', '2'),
          ('A', '2021-01-07', '2'),
          ('A', '2021-01-10', '3'),
          ('A', '2021-01-11', '3'),
          ('A', '2021-01-11', '3'),
          ('B', '2021-01-01', '2'),
          ('B', '2021-01-02', '2'),
          ('B', '2021-01-04', '1'),
          ('B', '2021-01-11', '1'),
          ('B', '2021-01-16', '3'),
          ('B', '2021-02-01', '3'),
          ('C', '2021-01-01', '3'),
          ('C', '2021-01-01', '1'),
          ('C', '2021-01-07', '3')]

sales_cols= ["customer_id", "order_date", "product_id"]
sales_df = spark.createDataFrame(data = sales_data, schema = sales_cols)

menu_data = [ ('1', 'palak_paneer', 100),
              ('2', 'chicken_tikka', 150),
              ('3', 'jeera_rice', 120),
              ('4', 'kheer', 110),
              ('5', 'vada_pav', 80),
              ('6', 'paneer_tikka', 180)]
# cols
menu_cols = ['product_id', 'product_name', 'price']

menu_df = spark.createDataFrame(data = menu_data, schema= menu_cols)

members_data = [ ('A', '2021-01-07'),
                 ('B', '2021-01-09')]
members_cols = ["customer_id", "join_date"]
members_df = spark.createDataFrame(data = members_data, schema = members_cols)

#What is the total amount each customer spent at the restaurant?
amountSpentPerCustomer_df = sales_df.join(menu_df,on='product_id').groupby('customer_id')\
    .agg(sum('price').alias('total_price')).orderBy('total_price',ascending=False)
#amountSpentPerCustomer_df.show()

#How many days has each customer visited the restaurant?
customerTotalDays_df = sales_df.groupby('customer_id').agg(countDistinct('order_date').alias('total_visits'))
#customerTotalDays_df.show()

#What was each customer’s first item from the menu?
window = Window.partitionBy('customer_id').orderBy('order_date')
firstItemPerCustomer_df = sales_df.withColumn('dense_rank',dense_rank().over(window))\
    .filter(col('dense_rank') == 1).join(menu_df,on = 'product_id').select('customer_id','product_id','product_name').orderBy('customer_id')
#firstItemPerCustomer_df.show()

#Find out the most purchased item from the menu and how many times the customers purchased it.
mostPurchasedItem_df = sales_df.join(menu_df,on = 'product_id').groupby('product_id','product_name')\
    .agg(count('product_id').alias('total_orders')).select('product_id','product_name','total_orders').orderBy('total_orders',ascending=False).limit(1)
#mostPurchasedItem_df.show()
#panda_df = mostPurchasedItem_df.toPandas()
#print(panda_df.iloc[panda_df['total_orders'].idxmax()])

#Which item was the most popular for each customer?
window1 = Window.partitionBy('customer_id').orderBy(col('total_count').desc())
mostPopularItem_df = sales_df.join(menu_df,on = 'product_id').groupby('customer_id','product_id','product_name').agg(count('*').alias('total_count'))\
    .withColumn('rank',dense_rank().over(window1)).filter(col('rank') == 1).select('customer_id','product_name','total_count')
    #.orderBy('total_count',ascending=False)
#mostPopularItem_df.show()

#Which item was ordered first by the customer after becoming a restaurant member
window2 = Window.partitionBy('customer_id').orderBy('order_date')
firstOrderPerCustomer = sales_df.join(members_df,on = 'customer_id').filter(sales_df.order_date >= members_df.join_date)\
    .withColumn('rank',dense_rank().over(window2)).filter(col('rank') == 1).join(menu_df,on = 'product_id').select('customer_id','product_name')
#firstOrderPerCustomer.show()

#Which item was purchased before the customer became a member
window3 = Window.partitionBy('customer_id').orderBy(col('order_date').desc())
orderBeforeMembership_df = sales_df.join(members_df,on = 'customer_id').filter(sales_df.order_date < members_df.join_date)\
    .withColumn('rank',dense_rank().over(window3)).filter(col('rank') == 1)\
    .join(menu_df,on = 'product_id').select('customer_id','product_name','price')
#orderBeforeMembership_df.show()

#What is the total items and amount spent for each member before they became a member
totalItemsAndAmount_df = sales_df.join(members_df, on = 'customer_id').filter(sales_df.order_date < members_df.join_date)\
    .join(menu_df, on ='product_id').groupby('customer_id').agg(countDistinct('product_id').alias('total_items'),sum('price').alias('amount_spent'))
#totalItemsAndAmount_df.show()

#If each rupee spent equates to 10 points and item ‘jeera_rice’ has a 2x points multiplier, find out how many points each customer would have.
pointsPerCustomer_df = sales_df.join(menu_df, on = 'product_id')\
    .withColumn('points',when(col('product_id') == 3,col('price') * 20).otherwise(col('price') * 10))\
    .groupby('customer_id').agg(sum('points').alias('total_points'))
#pointsPerCustomer_df.show()

#Create the complete table with all data and columns like customer_id, order_date, product_name, price, and member(Y/N)
newTable_df = sales_df.join(menu_df, on = 'product_id').join(members_df, on = 'customer_id', how= 'left')\
    .withColumn('member(Y/N)',when(col('join_date').isNotNull(),'Y').otherwise('N'))\
    .select('customer_id','order_date','product_name','price','member(Y/N)')
#newTable_df.show()

#We also require further information about the ranking of customer products. The owner does not need the ranking for non-member purchases,
# so he expects null ranking values for the records when customers still need to be part of the membership program.

window4 = Window.partitionBy('customer_id','is_member').orderBy('order_date')
ranking_df = sales_df.join(menu_df, on = 'product_id').join(members_df, on = 'customer_id', how = 'left')\
    .withColumn('is_member',when(col('order_date') < col('join_date'),'N').when(col('order_date') >= col('join_date'),'Y').otherwise('N'))\
    .withColumn('rank',when(col('is_member') == 'N',None).when(col('is_member') == 'Y',rank().over(window4)).otherwise(0))\
                .select('customer_id','order_date','product_name','price','is_member','rank')

ranking_df.show()