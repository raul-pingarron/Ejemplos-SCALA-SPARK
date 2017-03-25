# Ejemplos-SCALA-SPARK
Ejemplos de análisis en SCALA con SPARK

Para compilar y empaquetar: 
SPARK 1.6 :
 $ cd Spark_1.6
 $ sbt package
SPARK 2.1 :
 $ cd Spark_2.1
 $ sbt package

Los ficheros de trazas de Reddit pueden descargarse de https://ia801005.us.archive.org/19/items/2015_reddit_comments_corpus/reddit_data/

Para lanzar bajo HDFS:
 spark-submit --num-executors 64 --class "Analitica1" path_to_/analitica1_2.11-1.0.jar hdfs:///reddit-file
Para lanzar bajo NFS/local:
 spark-submit --num-executors 64 --class "Analitica1" path_to_/analitica1_2.11-1.0.jar file://_path_to_reddit-file
