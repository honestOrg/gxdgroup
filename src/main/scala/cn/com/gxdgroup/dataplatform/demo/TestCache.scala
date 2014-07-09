package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by wq on 14-5-30.
 */
object TestCache {

  def main(args: Array[String]){
//    val conf = new SparkConf()
//    conf.setMaster("spark://honest:7077")
//      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
//      .setAppName("wordcount")
//      .set("spark.executor.memory","2g").set("spark.default.parallelism","1")
    //.setJars(jars)

   // val sc1 = new SparkContext(conf)

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

    println("########################")
    val x = sc.parallelize(List(1,2,3))
    val cartesianComp = x.cartesian(x).map(x => (x))

    val l = sc.parallelize(List("1","2"))
    val kDistanceNeighbourhood = l.map(s =>
    {
      cartesianComp.filter(v => v != null)
    }
    )
    val kan = kDistanceNeighbourhood.take(20).mkString("|")
    println(kan)

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val rdd1 = sc.parallelize(Seq(
      ("1", "A"), ("2", "B"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"),
      ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"),
      ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"),
      ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"),
      ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C,b"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C"), ("3", "C")))
//    val rdd2 = sc.textFile("hdfs://cloud40:8020/user/gxd/lg/gxd/initializeRDDFile")
//
//    val rdd3 = rdd2.map{x=>
//      val sp = x.split(",")
//      ((sp(0),x))
//    }.groupByKey()
//
//    rdd3.take(3)
//
//    val m1 = rdd3.collectAsMap()
//    println("ddddddd:"+m1.get("001372A1-7D42-4444-B030-D0FC2FF51993").getOrElse("no!!"))



    val rdd1Broadcast = sc.broadcast(rdd1.groupByKey().collectAsMap())
    val m = rdd1Broadcast.value
    println("get:"+m.get("3").getOrElse("no case get"))
    println(m.get("5").foreach{x =>
      println(x)
      println(x.toArray.size)
      x.toArray.map(println _)
    })
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val rdd5 = m.get("5")
    rdd5.foreach(x =>
      println(x.toArray.size)
    )

    val lll = List(1,2,3,4)
    println(lll(3))

    val ss= List(("7","aaaa"),("8","xxxxx"),("8","ccccc"),("7","vvvvv"),("8","zzzz"),("6","wwwww"),("6","yyyyyy"))

    val ss1 = sc.parallelize(ss)

    ss1.map(_._2).saveAsTextFile("hdfs://cloud40:8020/user/gxd/wq/test")
  }

}
