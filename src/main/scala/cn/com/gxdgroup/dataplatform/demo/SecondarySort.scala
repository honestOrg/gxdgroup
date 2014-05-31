package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import com.redis.RedisClient
import org.apache.spark.api.java.JavaRDD

/**
 * Created by wq on 14-5-9.
 */
object SecondarySort {

  def main(args: Array[String]){
    val conf = new SparkConf()
    conf.setMaster("spark://honest:7077")
      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
      .setAppName("secondarySort")
      .set("spark.executor.memory","2g")
      //.setJars(jars)

    //val sc = new SparkContext(conf)
    val sc = new SparkContext("local","my hadoop file",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    val data = Array[(String,Int,Int)](
      ("x", 2, 9), ("y", 2, 5),
      ("x", 1, 3), ("y", 1, 7),
      ("y", 3, 1), ("x", 3, 6),
      ("a", 3, 1), ("b", 3, 6)
    )

    val data1 = List("hello world", "hello hehe world hehe haha haha")


    val pairs = sc.parallelize(data,3).persist(StorageLevel.MEMORY_ONLY)
    val test = pairs.map(k => (k._1,(k._2,k._3)))
    val reducerNumber = 3

    val result = test.groupByKey(reducerNumber).map(K => (K._1, K._2.sortBy(timeValue => timeValue._1)))

    //result.foreach(println)

    result.sortByKey().collect().map(println _)

    println("--------------------")
    val map1 = Map[String,String]("xx" -> "xx","cc"->"cc");
    val map2 = Map[String,String]("xx1" -> "xx1","cc1"->"cc1");
    val map = List("1"-> map1,"2"-> map2)


    val kankan = sc.parallelize(map).cache()
    //val kk = kankan.filter(line => line._1=="xx1").take(1).head._2
    //println("kk:"+kk)
    println("count:"+kankan.count())
    kankan.foreach({x => println(x._2);val bb = x._2;println(bb.get("xx"))})
    println("00000000000000000000000000")

    //--------------------------------------------
//    val r = new RedisClient("cloud41", 6379)
//    val redismap = r.hgetall("test3")
//    val redislist = List(redismap)
//    val kankanredis = sc.parallelize(redislist)
//    kankanredis.foreach({x => println(x.getOrElse(""));println(x.get("1"))})


    //---------------------------------------------
    val wqlist = List(("11","11"),("22","22"))
    val kankan2 =  sc.parallelize(wqlist).cache()
    val kk2 = kankan2.filter(x => x._1=="11").take(1).head._2
    println("kk2:"+kk2)
    println("kk2 of list:"+kk2.toList)

    //invoke java
    val slist = List("hello world","hehe hehe")

    val kankan3 = JavaWorldCount.getStringJavaRDD(sc.parallelize(slist).toJavaRDD())
    println(kankan3.toArray())

  }

}
