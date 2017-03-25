# Ejemplos-SCALA-SPARK
Ejemplos de análisis en SCALA con SPARK

Para compilar y empaquetar: 
 $ sbt package

Para lanzar:
 spark-submit --num-executors 64 --class "Analitica1" path_to_/analitica1_2.11-1.0.jar hdfs:///reddit-file
