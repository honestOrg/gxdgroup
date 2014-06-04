package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wq on 14-5-30.
 */
object TestCache {

  def main(args: Array[String]){
    val conf = new SparkConf()
    conf.setMaster("spark://honest:7077")
      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
      .setAppName("wordcount")
      .set("spark.executor.memory","2g").set("spark.default.parallelism","1")
    //.setJars(jars)

    val sc1 = new SparkContext(conf)

    val sc = new SparkContext("local","my hadoop file",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))

    val list = List(1,2,3,4)

    val data = sc.parallelize(list)

    val kankan = data.map{x=> println("aaa");x+1}.cache()

    //val cache = kankan.cache()

    val count = kankan.count()

    println(count)

    println("--------------------")

    println(kankan.first())
    println(kankan.collect().map(println _))

  }

}
