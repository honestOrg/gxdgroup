package cn.com.gxdgroup.dataplatform.avm.function.verson4

import org.apache.spark.SparkContext
import cn.com.gxdgroup.dataplatform.avm.model.{Setting, Section}
import cn.com.gxdgroup.dataplatform.avm.utils.{AVMUtils, JedisUtils}
import redis.clients.jedis.{Pipeline, Jedis}
import org.apache.spark.SparkContext._
/**
 * Created by ThinkPad on 14-7-1.
 */
object shenzhenshiInit {
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
    //因为来的数据是按城市分目录的,假设数组下标是1的为城市名，我们想构造的是redis中存的表名“北京市小区”和”北京市案例“
    val cityName = args(0).split("/")(2)
    val cityNametable = cityName+"小区表"
    val bargainNametale= cityName+"案例表"

    // val COMMUNITYLOAD:String = "COMMUNITYINIT"
    //val BARGAINSLOAD:String = "BARGAINSINIT"
    JedisUtils.initPool
    val j: Jedis = JedisUtils.getJedis

    if(j.exists(cityNametable)){
      j.del(cityNametable)
    }

    if(j.exists(bargainNametale)){
      j.del(bargainNametale)
    }




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
            communityArray(1)+"\t"+
            communityArray(2)+"\t"+
            communityArray(3)+"\t"+
            communityArray(4)+"\t"+
            communityArray(5)+"\t"+
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
            communityArray(24)+"\t"+
            communityArray(25)+"\t"+
            communityArray(26)
        pipe.hset(cityNametable, communityArray(0), communityDetail)
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
          BargainDetail_Arrays(3)+"\t"+
          BargainDetail_Arrays(4)+"\t"+
          BargainDetail_Arrays(5)+"\t"+
          BargainDetail_Arrays(6)+"\t"+
          BargainDetail_Arrays(7)+"\t"+
          BargainDetail_Arrays(8)+"\t"+
          BargainDetail_Arrays(9)+"\t"+
          BargainDetail_Arrays(10)+"\t"+
          BargainDetail_Arrays(11)+"\t"+
          BargainDetail_Arrays(12)+"\t"+
          BargainDetail_Arrays(13)+"\t"+
          BargainDetail_Arrays(14)
        val communityID = BargainDetail_Arrays(3)
        (communityID,bargainDetail)
    }.groupByKey()

    bargainKeyValues.foreachPartition{iter => {
      JedisUtils.initPool
      val j: Jedis = JedisUtils.getJedis
      val pipe: Pipeline = j.pipelined
      iter.foreach{line =>{
        pipe.hset(bargainNametale,line._1,line._2.mkString(","))

      }
        pipe.sync
      }
    }

    }
  }
}
