# Ejemplos-SCALA-SPARK
Ejemplos de análisis en SCALA con SPARK

Para compilar y empaquetar: 
SPARK 1.6 :
 $ cd Spark_1.6
 $ sbt package
SPARK 2.1 :
 $ cd Spark_2.1
 $ sbt package

Descargar el fichero de ejemplo con las trazas de Reddit y decomprimirlo

Para lanzar bajo HDFS:
 spark-submit --num-executors 64 --class "Analitica1" path_to_/analitica1_2.11-1.0.jar hdfs:///reddit-file
Para lanzar bajo NFS/local:
 spark-submit --num-executors 64 --class "Analitica1" path_to_/analitica1_2.11-1.0.jar file://_path_to_reddit-file
