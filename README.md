# Ejemplos-SCALA-SPARK
Ejemplos de análisis en SCALA con SPARK

Los ficheros de trazas de Reddit pueden descargarse de https://ia801005.us.archive.org/19/items/2015_reddit_comments_corpus/reddit_data/

Para lanzar bajo HDFS:
 spark-submit --num-executors 64 --class "Analitica1" analitica1_2.11-1.0.jar hdfs:///reddit-file
