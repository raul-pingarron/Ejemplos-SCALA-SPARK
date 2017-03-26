# Ejemplos-SCALA-SPARK
Ejemplos de análisis en SCALA con SPARK

Los ficheros de trazas de Reddit pueden descargarse de https://ia801005.us.archive.org/19/items/2015_reddit_comments_corpus/reddit_data/

Para lanzar bajo HDFS:
``` 
spark-submit --num-executors 64 --class "Analitica1" analitica1_XX-YY.jar hdfs:///reddit-file
```
* ##### NOTA 1:
El ejemplo para SPARK 2.1 funciona con Scala 2.11

* NOTA 2:
Estos ejemplos están pensados para generar un número importante de operaciones de shuffle con el fin de amplificar la IO a disco, siempre y cuando el numero o tamaño de los ficheros de trazas de Reddit sea suficientemente grande.
De la misma manera el impacto en IO a disco en operaciones de lectura es severo, habiéndose generado en distintas pruebas que se han ejecutado througputs de 3 GiB/s (3072 MiB/s) sostenidos durante los 15-20 minutos que ha tarda la ejecución del job.
