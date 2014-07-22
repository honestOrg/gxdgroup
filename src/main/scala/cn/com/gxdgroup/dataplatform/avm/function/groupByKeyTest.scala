package cn.com.gxdgroup.dataplatform.avm.function
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapred.TextInputFormat
import java.util.{Random, Calendar, Date}
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate2

import cn.com.gxdgroup.dataplatform.avm.model.Bargain
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import cn.com.gxdgroup.dataplatform.avm.model.Setting
import cn.com.gxdgroup.dataplatform.avm.model.AVMCommunity
import cn.com.gxdgroup.dataplatform.avm.model.Index
import cn.com.gxdgroup.dataplatform.avm.model.Result
import cn.com.gxdgroup.dataplatform.avm.model.Section
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import cn.com.gxdgroup.dataplatform.avm.model.AVMBargain
import org.apache.spark.storage.StorageLevel
import org.apache.commons.lang.StringUtils
import scala.collection.mutable
import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}
import scala.concurrent.{Await, Future}
import scala.BigDecimal
import cn.com.gxdgroup.dataplatform.avm.utils.{JedisUtils, AVMUtils}
import scala.collection.mutable.ArrayBuffer
import redis.clients.jedis.{Pipeline, Jedis, Tuple}
/**
 * Created by ThinkPad on 14-6-18.
 */
object groupByKeyTest {
  def main(args: Array[String]){

    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))


    val textfile = sc.parallelize(Seq((1,"1,2,a1"),(1,"1,4,a2"),(1,"1,6,a3"),(2,"2,3,b1"),(2,"2,6,b2"),(2,"2,9,b3"),(3,"3,12,c1")),args(0).toInt)
/*
  val groupByKeyRDD = textfile.groupByKey()

    groupByKeyRDD.foreach{line=>
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      val num = new Random().nextInt(10000)
      line._2.map{line=>
          j.hset(num.toString,line,"abc")
      }

    }
 */

    val textfileGroupBy = textfile.groupByKey()

    val tRDD = textfileGroupBy.flatMap{line =>
     // val lineDetail = sc.parallelize(line._2)
//     val carrestianVal =  lineDetail.cartesian(lineDetail).map{line =>
//          println("line._1:"+line._1)
//          println("line._2:"+line._2)
//     }
  //  line._2

    val result = line._2.flatMap{kk =>

     val kk_arrays =  kk.split(",")
      line._2.map{hh=>
       val hh_arrays =  hh.split(",")
        (kk_arrays(0),kk_arrays(1).toInt-hh_arrays(1).toInt+"|##"+kk_arrays(0))
      }
    }
      result
    }
   // tRDD.groupByKey().saveAsTextFile("lg/gxd/groupbyKeyTest")
val groupBykeyVal =  tRDD.groupByKey()

    groupBykeyVal.foreach{line =>

      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      val pipe: Pipeline = j.pipelined
      val num = line._1
    line._2.foreach{ hh=>{
      pipe.hset(num,line._1,line._2.mkString(","))
    }
      pipe.sync()
    }

    }


    /*
      groupByKeyRDD.foreachpartition foreach{line=>
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      val num = new Random().nextInt(10000)
      line._2.map{line=>
          j.hset(num.toString,line,"abc")
      }

    }
     */



  }

}
