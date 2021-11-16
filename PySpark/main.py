from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, when, substring, locate, concat, lit, current_timestamp






spark = SparkSession.builder.getOrCreate()


netflix = spark.read.parquet('OriginaisNetflix.parquet')


netflix = netflix.withColumn('Premiere', 
                     when(
                             locate('-',col('Premiere'))==2, 
                             concat(
                                     lit('0'),
                                     col('Premiere')
                                     )
                             ).otherwise(col('Premiere'))
                    )


netflix = netflix.withColumn('Premiere', when(substring(col('Premiere'),4,3)=='Jan', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-01-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Feb', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-02-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Mar', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-03-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Apr', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-04-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='May', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-05-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Jun', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-06-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Jul', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-07-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Aug', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-08-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Sep', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-09-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Oct', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-10-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Nov', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-11-'),substring(col('Premiere'),1,2)))\
                           .when(substring(col('Premiere'),4,3)=='Dec', 
                                 concat(lit('20'),substring(col('Premiere'),8,2),lit('-12-'),substring(col('Premiere'),1,2)))\
                   
                   )
                   


netflix = netflix.withColumn('Premiere', col('Premiere').cast(TimestampType()))

netflix = netflix.withColumn('dt_inclusao', col('dt_inclusao').cast(TimestampType()))



netflix = netflix.sort(col("Active").desc(),col("Genre").asc())

netflix = netflix.dropDuplicates(netflix.columns)


netflix = netflix.withColumn('Seasons', when(col('Seasons')=='TBA','a ser anunciado').otherwise(col('Seasons')))

netflix = netflix.withColumn('Data de Alteração', current_timestamp())


rename_list = [('Title', 'Título'),
 ('Genre', 'Gênero'),
 ('GenreLabels', 'Rótulos de gênero'),
 ('Premiere', 'Pré estreia'),
 ('Seasons', 'Temporadas'),
 ('SeasonsParsed', 'Temporadas Analisadas'),
 ('EpisodesParsed', 'Episódios Analisados'),
 ('Length', 'Duração'),
 ('MinLength', 'Duração mínima'),
 ('MaxLength', 'Duração máxima'),
 ('Status', 'Status'),
 ('Active', 'Ativo'),
 ('Table', 'Mesa'),
 ('Language', 'Língua'),
 ('dt_inclusao', 'Data Inclusão')]


for i in rename_list:
    netflix = netflix.withColumnRenamed(i[0],i[1])


m = netflix.toPandas()


netflix.coalesce(1).write.mode('overwrite').option("header","true").option("sep",";").csv('PySpark/out') #se gravado em hdsf
df = netflix.toPandas()
df.to_csv('PySpark/out.csv', sep=';', index=False)



#Não possuo conta na AWS para essa parte 8.
