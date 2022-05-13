from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

conf = SparkConf().setAppName('Step_four').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
df = spark.read.parquet('Store/inventory{}'.format('2022-04'))
df = spark.sql("select purchase_date, item, quantities, price from inventory where purchase_date > '2022-04-01' and purchase_date < '2022-05-01")
df.createOrReplaceTempView('inventory_list')
mov_avg_df = spark.sql("""
select t1.purchase_date, AVG(t2.price) as mov_avg_pr
from tmp_trade_moving_avg t1
left outer join tmp_trade_moving_avg t2 
    on t2.purchase_date between DATE_ADD(t1.purchase_date, interval -6 day) 
        and t1.'date'
group by t1.'date'
""")
mov_avg_df.write.saveAsTable("temp_trade_moving_avg")

date = datetime.strptime('2022-05-11', '%Y-%m-%d')
prev_date_str = date + timedelta(days=-1)
df = spark.sql("select purchase_date, item, quantities, price from inventory where purchase_date = {}".format(prev_date_str))
df.createOrReplaceTempView("tmp_last_trade")

quote_update = spark.sql("""
select purchase_date, item, quantities, price from inventory
from quote_union_update
where rec_type = 'Q'
""")
quote_update.createOrReplaceTempView("quote_update")

quote_update.write.parquet('Store/inventory{}'.format(prev_date_str))
