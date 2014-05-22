package cn.com.gxdgroup.dataplatform.avm.function

import java.util.Date
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.{Text, LongWritable, IntWritable}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import cn.com.gxdgroup.dataplatform.avm.utils.Utils
import cn.com.gxdgroup.dataplatform.avm.model.Community
import cn.com.gxdgroup.dataplatform.avm.model.Setting
import cn.com.gxdgroup.dataplatform.avm.model.Index
import cn.com.gxdgroup.dataplatform.avm.model.Bargain
import cn.com.gxdgroup.dataplatform.avm.model.Section
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import cn.com.gxdgroup.dataplatform.avm.model.AVMBargain
import org.apache.commons.lang.StringUtils
import java.util.Calendar

/**
 * Created by ThinkPad on 14-5-20.
 */
object GxdPriceDriver {
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
    val GetAllBargainTextFile = sc.textFile(args(3))

    //AllThreshold存为key，val的RDD，其中key是阈值系数名称，val是一行值，阈值系数名称, 楼盘类型匹配阈值 , 土地等级匹配阈值 , 公交数量匹配阈值 , 停车场匹配阈值, 周边配套匹配阈值, 交通管制匹配阈值,
    // 有利因素匹配阈值 , 不利因素匹配阈值 , 楼盘规模匹配阈值 , 建成年份匹配阈值 , 供热情况匹配阈值, 学区匹配阈值 , 楼盘风格匹配阈值, 物业等级匹配阈值 ,绿化率匹配阈值  , 建筑密度匹配阈值 , 容积率匹配阈值 , 距市中心距离匹配阈值, 距商服中心距离匹配阈值
    val thresholdArray= GetAllThreshold.map(line => {
      //先假设是逗号分隔
      val tt = line.split(",")
      //此处还需要修改,其中tt(1)代表的是阈值的名
      (tt(1),  Threshold(tt(0).toInt,tt(1),tt(2).toInt))
    })
    //初始化的时候直接输出
    val threadTarget = thresholdArray.filter(lines => lines._1.equals("测试阈值")).first()

    //其中key是城市，val是城市、价格 、时间
    //注意这个Date有可能不对
    val AllIndexMapList= GetAllIndex.map(line => {
      //先假设是逗号分隔
      val IndexDetail_Arrays = line.split(",")
      val index = Index(IndexDetail_Arrays(0).toInt, IndexDetail_Arrays(1),BigDecimal(IndexDetail_Arrays(2)),new Date("2014-05-19 14:40:29"))
      val cityName = IndexDetail_Arrays(1)
      (cityName,index)
    }).groupByKey().cache()

    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))

    val settingBroadCast = sc.broadcast(m_Setting)

    //  GetAllCommunity***********
    val AllCommunity=GetAllCommunity.map(line =>{
      //先假设是逗号分隔
      val target = line.split(",")
      var community= new Community(target(0),target(1),target(2),target(3),target(4).toDouble,target(5).toDouble,null)
      //其中5,6代表的是经纬度的数据数组下标到时候还需要改,0下标代表的是小区名
      val SimilarCommunity =
        GetAllCommunity.map(desc =>(desc,Utils.GetDistance(target(5).toDouble,
          target(6).toDouble,desc.split(",")(5).toDouble,
          desc.split(",")(6).toDouble))).filter(c =>(!c._1.split(",")(0).
          equals(target(0)))&&c._2<m_Setting("测试系数").maxDistance)
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
      //构造成具体的小区和它的相识的小区的键值对形式，其中line可以改成具体的小区名如target(1)
      (community,avmModel)
    })
    AllCommunity.cache()
    val AllCommunityBroadcast = sc.broadcast(AllCommunity)


    //取得所有Bargain，将其存在Map中其中key是小区名，value是list由所有的房子组成,注意要定义成var的
    val AllBargainMapList = GetAllBargainTextFile.map(line => {
      val BargainDetail_Arrays = line.split(",")
      //这个之后还需要改，不确定具体的字段对应
      val bargain = Bargain(BargainDetail_Arrays(0), BargainDetail_Arrays(1),
        BargainDetail_Arrays(2).toDouble,BargainDetail_Arrays(3).toInt,BargainDetail_Arrays(3).toInt,BargainDetail_Arrays(4),new Date(),BargainDetail_Arrays(5).toDouble)
      val communityID = BargainDetail_Arrays(0)
      (communityID,bargain)
    }).groupByKey().cache()
    val AllBargainMapListBroadCast = sc.broadcast(AllBargainMapList)


    def getResult(str:String):Result = {
      val rst = Result(0,null)
      val arrays = str.split(",")
      val CommunityName = arrays(0)
      val floor =arrays(1).toInt
      val totalFloor = arrays(2).toInt
      val square = arrays(3).toDouble
      val faceTo = arrays(4)
      val buildYear = arrays(5)
      val Location_Longitude:Double = 0
      val Location_Latitude:Double = 0
      rst
    }

    def process(pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
                pLocation_Longitude:Double,pLocation_Latitude:Double)={

      val  targetCommunity = AllCommunityBroadcast.value.filter(line =>(line._1.CommunityName.equals(pcommunityName))).take(1).toList

      if(!targetCommunity.isEmpty){
        //其实只有一个
        val  bargainList = AllBargainMapListBroadCast.value.filter(line =>
          line._1.equals(targetCommunity.head._1.CommunityID)).take(1).toList

        val AVMBargainList=bargainList.head._2.map(barg => {
          val avmBargain = AVMBargain()
          avmBargain.Case = barg
          avmBargain.Weight=1
          avmBargain.adjustPrice = barg.bargainPrice
          avmBargain

        })
        //下面是对本小区的过滤，首先是时间过滤、楼盘类型过滤开始**********

        val mineCoummunityBargain = bargainList.head._2.filter(line => (new Date().
          getTime-line.bargainTime.getTime)>= settingBroadCast.value("测试系数").maxMonth*3600).filter(
            (line => (line.totalFloor <= threadTarget._2.CommunityStyle
              ||pTotalFloor>threadTarget._2.CommunityStyle)
              &&(pTotalFloor<=threadTarget._2.CommunityStyle||
              line.totalFloor>threadTarget._2.CommunityStyle))
          )

        //(1,(1,a))楼层分段过滤开始**********
        val flRuleList = settingBroadCast.value("测试系数").floorRule.toList

        val flRulePoint_KeyVal = flRuleList.filter(flr =>pfloor >= flr._2.small&&pfloor<flr._2.big)

        val flRulePoint_Key = flRulePoint_KeyVal.map(line => line match{
          case (x:Int,y :Section) => x
          case _ => 0
        }).head

          var weight:Double =1
      //下面的这个是个Map
       val flCoefficientList = settingBroadCast.value("测试系数").floorCoefficient
        //楼层分段过滤开始
      val bargainFloorRule = mineCoummunityBargain.map(cb =>{
         val flr_KeyVal=  flRuleList.filter(frl=>
            cb.totalFloor>=frl._2.small&&cb.totalFloor<frl._2.big)
          val targetfloorIntKey = flr_KeyVal.isEmpty match {
            case true => 0
            case false => flr_KeyVal.head._1
          }

          weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
          (weight,cb)
         }).filter(_._1 != 0)

       //面积分段过滤开始
       val squareList = settingBroadCast.value("测试系数").squareRule.toList

        val pointsquare_KeyVal= squareList.filter(sL =>
            psquare >= sL._2.small&&psquare<sL._2.big )
      //界面输入的值计算
       val pointSqureInt = pointsquare_KeyVal.map(line => line match{
           case (x:Int,y: Section) => x
            //这个值是默认值
           case _ =>0
         }).head

      val bargainSquare =bargainFloorRule.map(bfr =>{
        val slTarget_KeyVal = squareList.filter(sL => bfr._2.square>= sL._2.small &&
          bfr._2.square < sL._2.big)

       val slTarget_KeyInt =  slTarget_KeyVal.map(line => line match{
          case(x:Int,y:Section) => x
          case _ => 0
        }).head
        //因为bfr._1是double的所以默认是0.0
        weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)
        (weight,bfr._2)
       }).filter(_._1 != 0)

        //楼栋

        //建成年份开始过滤, 输入是经过面积过滤后的
        var PointbuildYear =0
        var tagertbuildYear = 0
        val buildYearCofficent = settingBroadCast.value("测试系数").buildYearCoefficient
        val buildYearBargain = bargainSquare.map(bgs => {
          if(StringUtils.isNumeric(pbuildYear)&& StringUtils.isNumeric(bgs._2.BuildYear)){
            PointbuildYear = pbuildYear.toInt
            tagertbuildYear = bgs._2.BuildYear.toInt

            val buildCha = Math.abs(PointbuildYear-tagertbuildYear)
            val buildKey = buildYearCofficent.get(buildCha).getOrElse(0.0)
            weight = bgs._1 * buildKey
            (weight,bgs._2)
          }else{
            (0,bgs._2)
          }
        }).filter(_._1 != 0)

        //根据挂牌时间开始过滤
        val maxMonth = settingBroadCast.value("测试系数").maxMonth
        val timePowerBargain =settingBroadCast.value("测试系数").bargainTimePower
        val  c = Calendar.getInstance()
        val timeBargain = buildYearBargain.map(byg => {
          //这块会有一些问题，主要看你传入的日期是什么格式的
          weight = byg._1 * Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-byg._2.bargainTime.getYear)*12 + (c.get(Calendar.MONTH) - byg._2.bargainTime.getMonth)))*1.0/maxMonth,
            timePowerBargain)
          (weight,byg._2)
        }).filter(_._1 !=0 )
      // 根据面积开始过滤
      val square_adjust_coefficient =settingBroadCast.value("测试系数").squareAdjustCoefficient.takeRight(1000).toList




    }
  }

}
}


