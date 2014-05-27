package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wq on 14-5-26.
 */
object MapPartitons {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("spark://honest:7077")
      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
      .setAppName("secondarySort")
      .set("spark.executor.memory", "2g")
    //.setJars(jars)

    //val sc = new SparkContext(conf)
    val sc = new SparkContext("local", "my hadoop file", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val data = Array[(String,Int,Int)](
      ("x", 2, 9), ("y", 2, 5),
      ("x", 1, 3), ("y", 1, 7),
      ("y", 3, 1), ("x", 3, 6),
      ("a", 3, 1), ("b", 3, 6)
    )
    var i = 0
    var j = 0
    var x = 0

    val list = List(1,2,3,4)
    list.map{ y =>
      x+=1
    }
    println("x:"+x)
    val file = sc.parallelize(data,4)

    file.map{
      j+=1
      println(_)
    }.collect()

    println("j:"+j)

    val pp = file.mapPartitions{iter =>
      i+=1
      println("kankan:"+i)
      iter.map {
        x =>
          i += 1
          println("kankan1:"+i)
          println(x)
      };
      //println("kankan2:"+i)
    }.collect()

    println("i:"+i)
    println(pp)
  }

}
