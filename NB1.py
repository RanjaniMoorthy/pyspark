# Databricks notebook source
# MAGIC %md
# MAGIC Data Read

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')


# COMMAND ----------

# MAGIC %md
# MAGIC csv

# COMMAND ----------

df = spark.read.format('csv').option('inferschema','True').option('header','True').load('dbfs:/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC json

# COMMAND ----------

 df_json = spark.read.format('json').option('inferschema',True)\
   .option('header',True)\
     .option('multiline',False)\
       .load('dbfs:/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC schema - change datatype
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DDL Schema

# COMMAND ----------

myschema= '''
Item_Weight string
--add others
'''


# COMMAND ----------

# MAGIC %md
# MAGIC structType()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC select

# COMMAND ----------

df.select('item_identifier').display()

# COMMAND ----------

df_sel = df.select('item_identifier')
df_sel.display()

# COMMAND ----------

df.select(col('item_identifier')).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC alias

# COMMAND ----------

df.select(col('item_identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC filter

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')== 'Regular').display()

# COMMAND ----------

df.filter(col('item_type')=='soft_Drinks') and col('')

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC withcolumnrenamed
# MAGIC

# COMMAND ----------

df_rename = df.withColumnRenamed('Item_Weight','Item_wt')

# COMMAND ----------

df_rename.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC withcolumn

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('new_col',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","reg"))\
  .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Type Casting

# COMMAND ----------

 df.withColumn(('item_weight'),col('Item_weight').cast(StringType())).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC orderby/sort

# COMMAND ----------

df.display()

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Outlet_Establishment_Year').desc(),col('Outlet_Size').desc()).display()

# COMMAND ----------

df.sort(['Outlet_Establishment_Year','Outlet_Size'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Limit
# MAGIC

# COMMAND ----------

df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC drop

# COMMAND ----------

df.drop('item_visibility').display()

# COMMAND ----------

df.drop('item_visibility','item_weight').display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC drop duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df = df.withColumn('currentdt',current_date())
df.display()

# COMMAND ----------

df=df.withColumn('currentdt',date_format('currentdt','dd-MM-yyyy'))
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC handling nulls

# COMMAND ----------

#drop 
df.dropna('all')

# COMMAND ----------

df.drop('any')

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

#fill
df.fillNa('NA')

# COMMAND ----------

df.fillna('NA',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC split and Index

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------


dfn = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
dfn.display()

# COMMAND ----------

# MAGIC %md
# MAGIC exploed

# COMMAND ----------

dfn.withColumn('Outlet_Type',explode('Outlet_Type')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC arraycontainer

# COMMAND ----------

dfn.display()

# COMMAND ----------

dfn.withColumn('type_flag',array_contains('Outlet_Type','type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC group by

# COMMAND ----------

dfg = df.groupBy('Item_type').agg(sum('item_MRP'))
dfg.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupby('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('total')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC collect_List
# MAGIC

# COMMAND ----------

data = [
  ('user1','book1'),
  ('user1','book2')
]

schema = 'user string, book string'

df_b = spark.createDataFrame(data,schema)

df_b.display()

# COMMAND ----------

df_b.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC pivot

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC when-otherwise

# COMMAND ----------

df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non').otherwise('veg')).display()

# COMMAND ----------

df.withColumn('Flag',when((col('Item_Type')!='Meat') & (col('item_MRP')>200),'veg exp')\
  .when((col('Item_Type')!='Meat') & (col('item_MRP')<200),'veg not exp')\
    .otherwise('non veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Join

# COMMAND ----------

dataj1= [('1','A','D1'),
        ('2','B','D2'),
        ('3','C','D3'),
        ('4','D','D3'),
        ('5','E','D5')
]

schema1 = 'id string, name string, dep string'
dfj1 = spark.createDataFrame(dataj1,schema1)

dataj2 = [('D1','HR'),
          ('D2','Mark'),
          ('D3','acc'),
          ('D4','IT'),
          ('D5','Fin')
]

schema2 = 'dep string, dep_name string'
dfj2 = spark.createDataFrame(dataj2,schema2)

# COMMAND ----------

dfj1.display()
dfj2.display()

# COMMAND ----------

dfj1.join(dfj2,dfj1['dep']==dfj2['dep'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC window funcitons

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowfun',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC cumulative sum

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC udf

# COMMAND ----------

def my_func(x):
  return x*x

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('udf_f',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC data wrirting

# COMMAND ----------

#csv write csv into stroage
df.write.format('csv')\
  .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/CSV')

# COMMAND ----------

df = spark.read.format('csv').option('inferschema','True').option('header','True').load('dbfs:/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.display()


# COMMAND ----------

#parquet
df.write.format('parquet')\
  .mode('overwrite')\
  .option('path','/FileStore/tables/CSV/data.csv')\
  .save()
