package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wq on 14-5-9.
 */
object SecondarySort {

  def main(args: Array[String]){
    val conf = new SparkConf()
    conf.setMaster("spark://honest:7077")
      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
      .setAppName("wordcount")
      .set("spark.executor.memory","2g")
    //.setJars(jars)

    val sc = new SparkContext(conf)
    val data = Array[(String,Int,Int)](
      ("x", 2, 9), ("y", 2, 5),
      ("x", 1, 3), ("y", 1, 7),
      ("y", 3, 1), ("x", 3, 6),
      ("a", 3, 1), ("b", 3, 6)
    )

    val pairs = sc.parallelize(data,3)
    val test = pairs.map(k => (k._1,(k._2,k._3)))
    val reducerNumber = 3

    val result = test.groupByKey(reducerNumber).map(K => (K._1, K._2.sortBy(timeValue => timeValue._1)))

    //result.foreach(println)

    result.sortByKey().collect
  }

}
