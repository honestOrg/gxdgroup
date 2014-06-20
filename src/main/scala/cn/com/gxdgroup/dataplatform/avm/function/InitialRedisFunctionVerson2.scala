package cn.com.gxdgroup.dataplatform.avm.function
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapred.TextInputFormat
import java.util.{Calendar, Date}
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
import cn.com.gxdgroup.dataplatform.avm.utils.JedisUtils
import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}
import scala.concurrent.{Await, Future}
import scala.BigDecimal
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
import scala.collection.mutable.ArrayBuffer
import redis.clients.jedis.{Pipeline, Jedis, Tuple}
/**
 * Created by ThinkPad on 14-6-19.
 */
object InitialRedisFunctionVerson2 {
  def main(args: Array[String]){
    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    //jedis初始化
    //SETTING初始化
    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))
    val GetAllCommunity16 = sc.textFile(args(0),args(1).toInt)

    val getAllBargains = sc.textFile(args(2),args(3).toInt)
    //redis初始化

    val AVMLOAD:String = "AVMINIT"
    val COMMUNITYLOAD:String = "COMMUNITYINIT"
    val BARGAINSLOAD:String = "BARGAINSINIT"
    /*
            if(j.exists(AVMLOAD)){
              j.del(AVMLOAD)
            }
            if(j.exists(COMMUNITYLOAD)){
              j.del(COMMUNITYLOAD)
            }
            if(j.exists(BARGAINSLOAD)){
              j.del(BARGAINSLOAD)
            }
    */
  val commIDAndDetal =   GetAllCommunity16.map{line =>
      val communityDetail = line.split("\t")
      (communityDetail(0),line)
    }.groupByKey()

    commIDAndDetal.map{line =>
      val communityDetal =sc.parallelize(line._2)
     val cartesianVal =  communityDetal.cartesian(communityDetal).filter{line =>
        line._1.split("\t")(0)!=line._2.split("\t")(0)
      }.map{
        line =>
          val target1 = line._1.split("\t")
          val target2 = line._2.split("\t")
          val distance = AVMUtils.GetDistance(target1(5).toDouble,
            target1(4).toDouble,
            target2(5).toDouble,
            target2(4).toDouble)
          (distance,target1(0)+"\t"+target2(0))
      }.filter(line => line._1<m_Setting("测试系数").maxDistance).map{line =>
        var lineArray = line._2.split("\t")
        (lineArray(0),line._1+"\t"+lineArray(1))

     }.groupByKey()

      cartesianVal

    }

    val initialCartesian = GetAllCommunity16.cartesian(GetAllCommunity16).filter{line =>
      line._1.split("\t")(0)!=line._2.split("\t")(0)
    }.map{
      line =>
        val target1 = line._1.split("\t")
        val target2 = line._2.split("\t")
        val distance = AVMUtils.GetDistance(target1(5).toDouble,
          target1(4).toDouble,
          target2(5).toDouble,
          target2(4).toDouble)
        (distance,target1(0)+"\t"+target2(0))
    }.filter(line => line._1<m_Setting("测试系数").maxDistance).map(line =>{
      var lineArray = line._2.split("\t")
      (lineArray(0),line._1+"\t"+lineArray(1))

    }).groupByKey()


    //println("start:!!!!!!!!!!!!111111")
    initialCartesian.foreachPartition{iter=> {
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      // println("okkkkkkkkkkkkkkkkkkkkkkkkkk")
      val pipe: Pipeline = j.pipelined
      iter.foreach{line =>{
        //    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        pipe.hset("AVMINIT", line._1, line._2.mkString(","))
      }
        pipe.sync
      }
    }}
    //    JedisUtils.initPool
    //    val j: Jedis = JedisUtils.getJedis
    //     val communityIdMap = j.hgetAll("AVMINIT")
    //    println("size:"+communityIdMap.size())

    //小区初始化，构造成CommunityID，CommunityDetail
    GetAllCommunity16.foreachPartition{iter =>{
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      //
      val pipe: Pipeline = j.pipelined
      iter.foreach{line =>{
        val communityArray = line.split("\t")
        var communityDetail =
          communityArray(0)+"\t"+
            communityArray(6)+"\t"+
            communityArray(7)+"\t"+
            communityArray(8)+"\t"+
            communityArray(9)+"\t"+
            communityArray(10)+"\t"+
            communityArray(11)+"\t"+
            communityArray(12)+"\t"+
            communityArray(13)+"\t"+
            communityArray(14)+"\t"+
            communityArray(15)+"\t"+
            communityArray(16)+"\t"+
            communityArray(17)+"\t"+
            communityArray(18)+"\t"+
            communityArray(19)+"\t"+
            communityArray(20)+"\t"+
            communityArray(21)+"\t"+
            communityArray(22)+"\t"+
            communityArray(23)+"\t"+
            communityArray(24)
        pipe.hset(COMMUNITYLOAD, communityArray(0), communityDetail)
        // j.hset("COMMUNITYINIT", communityArray(0), communityDetail)
      }
        pipe.sync
      }
    }}

    //bargain的初始化
    val bargainKeyValues = getAllBargains.map{
      line=>
        val BargainDetail_Arrays = line.split("\t")
        val bargainDetail = BargainDetail_Arrays(0)+"\t"+
          BargainDetail_Arrays(1)+"\t"+
          BargainDetail_Arrays(2)+"\t"+
          BargainDetail_Arrays(4)+"\t"+
          BargainDetail_Arrays(5)+"\t"+
          BargainDetail_Arrays(8)+"\t"+
          BargainDetail_Arrays(9)+"\t"+
          BargainDetail_Arrays(10)
        val communityID = BargainDetail_Arrays(1)
        (communityID,bargainDetail)
    }.groupByKey()


    bargainKeyValues.foreachPartition{iter => {
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      val pipe: Pipeline = j.pipelined
      iter.foreach{line =>{

        pipe.hset(BARGAINSLOAD,line._1,line._2.mkString(","))

      }
        pipe.sync
      }
    }

      //Index初始化操作

    }
    // JedisUtils.close(j)
  }
}
