import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.joda.time._
import org.joda.time.format._

object Analitica1 {
  def main(args: Array[String]) {

  /**  Eliminamos logs innecesarios: */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length < 1) {
      System.err.println("Uso: Analitica1 <RUTA-HDFS_al_fichero>")
      System.exit(1)
    }

    val tinicio = System.currentTimeMillis()
    val spark = SparkSession
      .builder()
      .appName("Analitica1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    
    import spark.implicits._

    val fichero = spark.read.json(args(0))

  /**  Obtenemos el No entradas de la fuente de datos */
    println ()
    println ("El No de entradas de la fuente es de: "+fichero.count())
    println ()

  /**  Me paso este DataFrame a una tabla: */
    fichero.createOrReplaceTempView("tabla")

 /**  Consulta SQL para obtener el usuario mas activo     */
    val df1 = spark.sql("SELECT author FROM tabla WHERE author NOT LIKE '[deleted]'")
    val rdd1 = df1.rdd.map(x => (x,1)).reduceByKey(_+_).sortBy(x => -x._2) 
    val res1 = rdd1.first()
    val user1 = res1._1
    val posts = res1._2
    println ("El usuario mas activo es "+user1+" con "+posts+" posts")
    println ()

 /**  Consulta SQL para obtener el usuario mas antiguo que ha participado   */
    val df2 = spark.sql("SELECT author,created_utc FROM tabla ORDER BY created_utc")  
    val res2 = df2.first()
    val user2 = res2.getString(0)
    val msegs = res2.getString(1).toLong*1000
    val fecha = new DateTime(msegs).toDateTime.toString("dd/MM/yyyy")
    println ("El usuario ["+user2+"] es el mas antiguo y su 1er post lo hizo el "+fecha) 

 /** Consulta SQL para sacar el usuario con mas puntuacion: */
    val df3 = spark.sql("SELECT author,score FROM tabla WHERE author NOT LIKE '[deleted]'")
    val rddY = df3.rdd.map(x => (x(0).toString, x(1).toString.toLong)).reduceByKey(_+_).sortBy(x => -x._2)
    val res3 = rddY.first()
    val user3 = res3._1
    val puntuacion = res3._2
    println ("El usuario ["+user3+"] es el que mas scoring tiene con un total de "+puntuacion+" puntos")

 /** Consulta SQL para sacar los usuarios que entienden de motos:   */
    val df4 = spark.sql("SELECT author FROM tabla WHERE author NOT LIKE '[deleted]' AND body IS NOT NULL AND body LIKE '%motor%' AND body LIKE '%speed%' AND body LIKE '%Ducati%'")
    df4.rdd.repartition(1).saveAsTextFile("/user/rpingarron/moteros")

    val tfin = System.currentTimeMillis()

    println("El tiempo de ejecucion ha sido de : " + ((tfin-tinicio)/1000) + " segundos - " + (tfin-tinicio) + " ms - ")

    spark.stop()
    }
}

