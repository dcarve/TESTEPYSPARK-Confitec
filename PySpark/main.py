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

netflix = netflix.withColumn('Data de Altera????o', current_timestamp())


rename_list = [('Title', 'T??tulo'),
 ('Genre', 'G??nero'),
 ('GenreLabels', 'R??tulos de g??nero'),
 ('Premiere', 'Pr?? estreia'),
 ('Seasons', 'Temporadas'),
 ('SeasonsParsed', 'Temporadas Analisadas'),
 ('EpisodesParsed', 'Epis??dios Analisados'),
 ('Length', 'Dura????o'),
 ('MinLength', 'Dura????o m??nima'),
 ('MaxLength', 'Dura????o m??xima'),
 ('Status', 'Status'),
 ('Active', 'Ativo'),
 ('Table', 'Mesa'),
 ('Language', 'L??ngua'),
 ('dt_inclusao', 'Data Inclus??o')]


for i in rename_list:
    netflix = netflix.withColumnRenamed(i[0],i[1])


out = netflix.select('T??tulo','G??nero','Temporadas','Pr?? estreia', 'L??ngua', 'Ativo', 'Status', 'Data Inclus??o', 'Data de Altera????o')


out.coalesce(1).write.mode('overwrite').option("header","true").option("sep",";").csv('PySpark/out') #par gravar em hdsf
df = out.toPandas()
df.to_csv('PySpark/out.csv', sep=';', index=False) # para gravar direto no ambiente onde o python esta rodando

