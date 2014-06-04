package cn.com.gxdgroup.dataplatform.avm.function
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapred.TextInputFormat
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
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
import org.apache.spark.storage.StorageLevel
import org.apache.commons.lang.StringUtils
import scala.collection.mutable
import redis.clients.jedis.Tuple
import com.redis._

import serialization._

import Parse.Implicits._
import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Success
import scala.concurrent.duration._
import java.io.PrintWriter
import java.io.{File, PrintWriter}

import scala.math.random

/**
 * Created by ThinkPad on 14-6-3.
 */
object TestListValue {

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
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))

    val setting = sc.broadcast(m_Setting).value

    val GetAllCommunity = sc.textFile(args(0))
    val GetAllCommunity16 = sc.textFile(args(0),args(1).toInt)


    var SimilarCommunityList:List[String] =Nil
    val AllCommunityTo_Array = GetAllCommunity.toArray()
  //  println("GetAllCommunity16:@@@@@@@@@@@@@:"+GetAllCommunity16.count())

   val get16toArray =  GetAllCommunity16.toArray()
    val byMapPartition = GetAllCommunity16.mapPartitions(iter =>{

        iter.map(line =>{
          var target = line.split("\t")

          val SimilarCommunity =
            get16toArray.map(desc =>{
             // println("target:"+target)
              //println("desc#####:"+desc)
              val descArrays = desc.split("\t")
              ( descArrays(0),
                descArrays(6),
                descArrays(7),
                descArrays(8),
                descArrays(9),
                descArrays(10),
                descArrays(11),
                descArrays(12),
                descArrays(13),
                descArrays(14),
                descArrays(15),
                descArrays(16),
                descArrays(17),
                descArrays(18),
                descArrays(19),
                descArrays(20),
                descArrays(21),
                descArrays(22),
                descArrays(23),
                descArrays(24),
                AVMUtils.GetDistance(target(4).toDouble,
                  target(5).toDouble,
                  descArrays(4).toDouble,
                  descArrays(5).toDouble))
            }).filter(c =>(c._1 !=(target(0)))&&c._21<m_Setting("测试系数").maxDistance)


          SimilarCommunity.map(line => {
        //  println("line22222:"+line)
            //构造成的形式就是(要查找小区ID，要查找小区名字，相似小区ID，相似之间的距离(这里都写成String，以后要转为Double))
            // SimilarCommunityList= SimilarCommunityList ++ List[(target(0),target(1).toString,line._1.toString,line._2.toDouble,target(1).toString,line._1.toString,line._2.toDouble)]
            var result =
              target(0)+","+
                target(1)+","+
                target(6)+","+
                target(7)+","+
                target(8)+","+
                target(9)+","+
                target(10)+","+
                target(11)+","+
                target(12)+","+
                target(13)+","+
                target(14)+","+
                target(15)+","+
                target(16)+","+
                target(17)+","+
                target(18)+","+
                target(19)+","+
                target(20)+","+
                target(21)+","+
                target(22)+","+
                target(23)+","+
                target(24)+","+
                line._1+","+
                line._2+","+
                line._3+","+
                line._4+","+
                line._5+","+
                line._6+","+
                line._7+","+
                line._8+","+
                line._9+","+
                line._10+","+
                line._11+","+
                line._12+","+
                line._13+","+
                line._14+","+
                line._15+","+
                line._16+","+
                line._17+","+
                line._18+","+
                line._19+","+
                line._20+","+
                line._21.toString
            SimilarCommunityList = result::SimilarCommunityList
          })

      }
        )

    }).take(20000)



    SimilarCommunityList

    var initializeRDD = sc.parallelize(SimilarCommunityList)

    initializeRDD.saveAsTextFile("lg/gxd/initializeRDDFile")

  }

  }
