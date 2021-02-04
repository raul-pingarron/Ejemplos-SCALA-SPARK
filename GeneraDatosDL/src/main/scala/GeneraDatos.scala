/** Programa para generar ficheros vectoriales dispersos */
/** R.Pingarron <raul.ping4rr0n@gmail.com> */

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object GeneraDatos {

  def main(args: Array[String]) {

  /**  Eliminamos logs innecesarios: */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length < 1) {
      System.err.println("Uso: GeneraDatos <ruta_al_FS>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("GeneraDatos")
    val sc = new SparkContext(conf)

    var numFilas: Int = 40000000
    var numColumnas: Int = 2048
    var numParticiones: Int = 100000
    var dispersidad: Double = 0.5

    val rdd1: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, numFilas, numColumnas, numParticiones)
    val rango = new java.util.Random()
    val data = rdd1.map{v =>
      val a = Array.fill[Double](v.size)(0.0)
      v.foreachActive{(i,vi) =>
         if(rango.nextDouble <= dispersidad){
           a(i) = vi
         }
      }
      Vectors.dense(a).toSparse
   }

    data.saveAsObjectFile(args(0))

    sc.stop()
  }
}
