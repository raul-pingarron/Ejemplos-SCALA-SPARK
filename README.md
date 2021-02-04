# Ejemplos de codigo SCALA para ejecución sobre SPARK

## Analitica1
Un simple ejemplo de análisis masivo de datos provenientes de trazas de Reddit con SPARK 2.1

Los ficheros de trazas de Reddit pueden descargarse de
  https://ia801005.us.archive.org/19/items/2015_reddit_comments_corpus/reddit_data/

Para lanzar bajo HDFS:
``` 
spark-submit --num-executors 64 --class "Analitica1" analitica1_XX-YY.jar hdfs:///reddit-file
```
* NOTA 1:
El ejemplo es para SPARK 2.1 y funciona con Scala 2.11

* :warning: NOTA 2:
Este ejemplo genera un número importante de operaciones de shuffle con el fin de amplificar la IO a disco, siempre y cuando el numero o tamaño de los ficheros de trazas de Reddit sea suficientemente grande. 
De la misma manera el impacto en IO a disco en operaciones de lectura es severo, habiéndose generado en distintas pruebas que se han ejecutado througputs de 3 GiB/s (3072 MiB/s) sostenidos durante los 15-20 minutos que ha tarda la ejecución del job.

## Ejemplo micro-benchmark de generación dataset DeepLearning
Se encuentra en el directorio `GeneraDatosDL`.

Este ejemplo utiliza la MLlib de Spark para generar un vector disperso de enorme tamaño que después se persiste a disco en formato binario parcicionándolo el 100.000 ficheros de pequeño tamaño. ATENCION: Este job está pensado para estresar el subsistema de I/O y en especial la gestión y generación de metadatos (alocación de bloques, RPCs, etc.).
