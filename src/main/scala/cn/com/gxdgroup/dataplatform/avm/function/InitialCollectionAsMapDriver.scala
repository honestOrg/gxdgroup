package cn.com.gxdgroup.dataplatform.avm.function
import cn.com.gxdgroup.dataplatform.avm.model.Section
import org.apache.spark.SparkContext
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
import java.util.{Calendar, Date}
import cn.com.gxdgroup.dataplatform.avm.model.Setting
import com.redis._
import serialization._
import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}
import scala.concurrent.{Await, Future}
import java.io.{File, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
/**
 * Created by ThinkPad on 14-6-11.
 */
object InitialCollectionAsMapDriver {
  def main(args: Array[String]){
    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    //SETTING初始化
    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))

    val GetAllCommunity16 = sc.textFile(args(0),args(1).toInt)


   val cart =  GetAllCommunity16.cartesian(GetAllCommunity16).map{
      line =>
        val target1 = line._1.split("\t")
        val target2 = line._2.split("\t")
        val distance = AVMUtils.GetDistance(target1(4).toDouble,
          target1(5).toDouble,
          target2(4).toDouble,
          target2(5).toDouble)
        (distance,target1(0)+"\t"+
          target1(6)+"\t"+
          target1(7)+"\t"+
          target1(8)+"\t"+
          target1(9)+"\t"+
          target1(10)+"\t"+
          target1(11)+"\t"+
          target1(12)+"\t"+
          target1(13)+"\t"+
          target1(14)+"\t"+
          target1(15)+"\t"+
          target1(16)+"\t"+
          target1(17)+"\t"+
          target1(18)+"\t"+
          target1(19)+"\t"+
          target1(20)+"\t"+
          target1(21)+"\t"+
          target1(22)+"\t"+
          target1(23)+"\t"+
          target1(24)+"\t"+
          distance+"\t"+
          target2(0)+"\t"+
          target2(6)+"\t"+
          target2(7)+"\t"+
          target2(8)+"\t"+
          target2(9)+"\t"+
          target2(10)+"\t"+
          target2(11)+"\t"+
          target2(12)+"\t"+
          target2(13)+"\t"+
          target2(14)+"\t"+
          target2(15)+"\t"+
          target2(16)+"\t"+
          target2(17)+"\t"+
          target2(18)+"\t"+
          target2(19)+"\t"+
          target2(20)+"\t"+
          target2(21)+"\t"+
          target2(22)+"\t"+
          target2(23)+"\t"+
          target2(24))
    }.filter(line => line._1<m_Setting("测试系数").maxDistance)
      .map(line =>line._2).map(line => {
      var lines = line.split(",")
      (lines(0),line)
    })
   val collectAsMap =  cart.groupByKey().collectAsMap()
    val listParall = List("a","b")

    val listRDD = sc.parallelize(listParall)

    listRDD.map(line =>

      collectAsMap.get(line)

    ).count()

  }
}
