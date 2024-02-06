from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,DateType,DoubleType,LongType
from pyspark.sql.functions import col,round,mean,max,min,year,month,avg

spark = SparkSession.builder.appName('Walmart-stock-analysis').getOrCreate()

#Open,High,Low,Close,Adj Close,Volume
stock_schema = StructType([
    StructField('Date',DateType(),nullable=True),
    StructField('Open',DoubleType(),nullable=True),
    StructField('High',DoubleType(),nullable=True),
    StructField('Low',DoubleType(),nullable=True),
    StructField('Close',DoubleType(),nullable=True),
    StructField('Adj Close',DoubleType(),nullable=True),
    StructField('Volume',LongType(),nullable=True)
])
stock_df = spark.read.format('csv').option('header','true').schema(stock_schema).load('./Data/WMT.csv')

print(f'No. of rows : {stock_df.count()}')

print(f'Column names : {stock_df.columns}')

print(f'Schema : {stock_df.schema}')

print(f'Summary statistics : {stock_df.describe()}')

stock_df = stock_df\
    .withColumn('Open',col('Open').cast('decimal(10,2)'))\
    .withColumn('High',col('High').cast('decimal(10,2)'))\
    .withColumn('Low',col('Low').cast('decimal(10,2)'))\
    .withColumn('Close',col('Close').cast('decimal(10,2)'))\
    .withColumn('Adj_close',col('Adj Close').cast('decimal(10,2)'))\
    .drop(col('Adj Close'))\
    .withColumn('HV_Ratio',round(col('High')/col('Volume'),8))

#stock_df.show()

mean_df = stock_df.agg(mean('Close').alias('Mean_Close'))
mean_df.show()

max_min_df = stock_df.agg(max('Volume').alias('Max_Volume'),min('Volume').alias('Min_Volume'))
max_min_df.show()

close_lower_df = stock_df.filter(col('Close') < 60)
print(close_lower_df.count())

print(f"{stock_df.filter(col('High') > 80).count() / stock_df.count() * 100} %")

max_high_year_df = stock_df.groupby(year(col('Date')).alias('year'))\
    .agg(max(col('High')).alias('max_high')).orderBy(col('year'))
max_high_year_df.show()

avg_close_month_df = stock_df.groupby(month(col('Date')).alias('month'))\
    .agg(avg(col('Close')).alias('Avg_Close')).orderBy(col('month'))
avg_close_month_df.show()