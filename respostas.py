from pyspark.sql import HiveContext
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import count,col

h = HiveContext(sc)
df = h.sql("select * From data_set.logs_nasa")
df.show()

HOSTIS UNICOS
hostsUnicos = df.groupBy("host").count()
hostsUnicos.show()
 R: 137979 
erros404 = df.groupBy("retorno_http").count()
erros404.count()
 R: