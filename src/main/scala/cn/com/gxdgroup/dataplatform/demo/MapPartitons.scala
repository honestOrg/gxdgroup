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

    println("-----------------")
    val l1 = List(1,2,3,4)
    val l2 = List(5,6,7,8)
    val d1 = sc.parallelize(l1)
    val d2 = sc.parallelize(l2).cache()
    val a1 =  d2.toArray()
    val d3 = d1.mapPartitions{iter =>
      iter.map{ x=>
      //println("d2:"+d2.count())
      println("a1:"+a1.size)
      println("d1:"+x)
      //x+d2.count()
      }
    }

    d3.collect()

    println("--------------------")

    val list2 = List(("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1),
      ("a11",1),("a1",1),("a2",1),("a3",1),("a4",1),("a5",1),("a6",1),("a8",1),("a9",1),("a10",1),("a11",1)
      )
    val td = sc.parallelize(list2,2)

    val ccc = td.mapPartitions{iter =>

      //println("ii:"+ii)
      println("list2:"+list2.size)
      iter.map{x=>
        var ii =0
        list2.map{y=>
          ii+=x._2
          //println(ii)
        }
        (x._1,ii)
      }
    }.collect().mkString("|")

    println("td:"+ccc)


    val ss= List(("7",1),("8",2),("8",3),("7",4),("8",5),("6",5),("6",9))

    val ss1 = sc.parallelize(ss)

    val ss2 = ss1.groupBy(x=>x._1)

    println(ss2.collect().mkString("|"))

  }

}
