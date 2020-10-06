import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('local').getOrCreate()

columns = ['red','green','yellow']
fruits = [('apple','pear','banana'),('Ferrari','Honda','Mazda')]

df = spark.createDataFrame(data=fruits,schema=columns)
df.show()

#Get dataframe columns -> Returns a list
print('Getting dataframe columns')
df.schema.names

map_dict = {
    'red':'red_abc',
    'yellow':'yellow_abc',
    'green':'green_abc'
}

new_columns = df.schema.names

for k,v in map_dict.items():
    if k in new_columns:
        new_columns[new_columns.index(k)] = v

print('These are the new columns')
print(new_columns)

payload = df.collect()
df2 = spark.createDataFrame(data=payload, schema=new_columns)
df2.show()
