package cn.com.gxdgroup.dataplatform.avm.function

import org.apache.spark.SparkContext

/**
 * Created by ThinkPad on 14-6-6.
 */
object CaretionDriver {
  def main(args: Array[String]){

    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
      val community = sc.textFile(args(0),args(1).toInt)
      println("aaaaaaaaaaaaaaaaaaaaaa")
       community.cartesian(community).foreach(line => line)
    println("bbbbbbbbbbbbbbbbbbbbbbbbbb")
  }
}
