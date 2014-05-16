package cn.com.gxdgroup.dataplatform.avm.function

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.{Text, LongWritable, IntWritable}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, KeyValueTextInputFormat}

import cn.com.gxdgroup.dataplatform.avm.modle._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map
import cn.com.gxdgroup.dataplatform.avm.utils._
import cn.com.gxdgroup.dataplatform.avm.modle.Section
import cn.com.gxdgroup.dataplatform.avm.modle.Community
import cn.com.gxdgroup.dataplatform.avm.modle.Setting
import cn.com.gxdgroup.dataplatform.avm.modle.Threshold
import cn.com.gxdgroup.dataplatform.avm.modle.AVMCommunity

/**
 * Created by wq on 14-3-30.
 */
object GxdPrice {

  def main(args: Array[String]){

    if(args.length != 5){
      println("Usage : Input path output path")
      System.exit(0)
    }

    val sc = new SparkContext("spark://cloud40:7077", "gxd",
     System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val GetAllIndex = sc.textFile(args(0))
    val GetAllThreshold = sc.textFile(args(1))
    val GetAllCommunity = sc.textFile(args(2))
    val GetAllBargain = sc.textFile(args(3))
//AllThreshold存为key，val的RDD，其中key是阈值系数名称，val是一行值，阈值系数名称, 楼盘类型匹配阈值 , 土地等级匹配阈值 , 公交数量匹配阈值 , 停车场匹配阈值, 周边配套匹配阈值, 交通管制匹配阈值,
  // 有利因素匹配阈值 , 不利因素匹配阈值 , 楼盘规模匹配阈值 , 建成年份匹配阈值 , 供热情况匹配阈值, 学区匹配阈值 , 楼盘风格匹配阈值, 物业等级匹配阈值 ,绿化率匹配阈值  , 建筑密度匹配阈值 , 容积率匹配阈值 , 距市中心距离匹配阈值, 距商服中心距离匹配阈值
  val Threshold= GetAllThreshold.map(line => {
   //先假设是逗号分隔
    val tt = line.split(",")
  //此处还需要修改
    (tt(0) ->  Threshold(tt(0).toInt,"rrr"))
  })
//其中key是城市，val是城市、价格 、时间
    val AllIndex= GetAllIndex.map(line => {
      //先假设是逗号分隔
      val tt = line.split(",")
      (tt(0) -> tt)
    })
    /**
     *  ID = 0,
                    Name = "测试系数",
                    MaxMonth = 12,
                    MaxDistance =1000,
                    BargainTimePower = 0.8,
                    DiffrentBuilding = 0.75,
                    DiffrentCommunity = 0.75,
                    DistancePower = 0.9,
                val SquareRule:Map[Int, Section] ,
              val  SquareCoefficient:Map[Int, Double],
                  val  FloorRule:Map[Int, Section],
              val  FloorCoefficient:Map[Int, Double],
     val SquareAdjustCoefficient:Map[Double, Double] ,
     val BuildYearCoefficient:Map[Double, Double]

     */
   val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
   val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
   val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
   val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
   val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
   val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
   val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))
   val setBroadCast = sc.broadcast(m_Setting)

//  GetAllCommunity

    val AllCommunity=GetAllCommunity.map(line =>{

      //先假设是逗号分隔
      val target = line.split(",")
      //var comm= new Community(tt(0),tt(1),tt(2),tt(3),tt(4).toDouble,tt(5).toDouble)
      //其中5,6代表的是经纬度的数据数组下标到时候还需要改,0下标代表的是小区名
      val SimilarCommunity =
        GetAllCommunity.map(desc =>(desc,Utils.GetDistance(target(5).toDouble,
        target(6).toDouble,desc.split(",")(5).toDouble,
        desc.split(",")(6).toDouble))).filter(c =>(!c._1.split(",")(0).
        equals(target(0)))&&c._2<m_Setting("测试系数").MaxDistance)
      //下面avmModel
      val avmModel = SimilarCommunity.map(line => {
        val arrays = line._1.split(",")
        val community = Community(arrays(0),arrays(1),arrays(2),arrays(3),arrays(4).toDouble,arrays(5).toDouble,null)
        val distance:Double = line._2
        val avmCommunity = AVMCommunity()
        avmCommunity.Community = community
        avmCommunity.Distance = distance
        avmCommunity
      })



    })


 val AllThresholdBroadcast = sc.broadcast(Threshold)
 val AllIndexBroadcast = sc.broadcast(AllIndex)

 //implicit  def IntToDouble(x:Int)=x.toDouble
  }
}