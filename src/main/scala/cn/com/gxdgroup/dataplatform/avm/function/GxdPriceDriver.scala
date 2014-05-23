package cn.com.gxdgroup.dataplatform.avm.function

import java.util.{Date, Calendar}
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
import org.apache.spark.rdd.RDD

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
      (tt(1),  Threshold(tt(0).toInt,tt(1),tt(2).toInt,tt(3).toDouble,tt(4).toDouble,tt(5).toDouble,tt(6).toDouble,tt(7).toDouble,tt(8).toDouble,tt(9).toDouble,tt(10).toDouble,tt(11).toDouble,tt(12).toDouble,tt(13).toDouble,tt(14).toDouble,tt(15).toDouble,tt(16).toDouble,tt(17).toDouble,tt(18).toDouble,tt(19).toDouble,tt(20).toDouble,tt(21).toDouble))
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

    val beijingIndexList = AllIndexMapList.filter(aiml=> aiml._1.equals("北京")).take(1).head._2.toList

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
      var community= new Community(target(0),target(1),target(2),target(3),target(4).toDouble,target(5).toDouble,BigDecimal(target(6)),BigDecimal(target(7)),BigDecimal(target(8)),BigDecimal(target(9)),BigDecimal(target(10)),BigDecimal(target(11)),BigDecimal(target(12)),BigDecimal(target(13)),BigDecimal(target(14)),BigDecimal(target(15)),BigDecimal(target(16)),BigDecimal(target(17)),BigDecimal(target(18)),BigDecimal(target(19)),BigDecimal(target(20)),BigDecimal(target(21)),BigDecimal(target(22)),BigDecimal(target(23)),BigDecimal(target(24)),null)
      //其中5,6代表的是经纬度的数据数组下标到时候还需要改,0下标代表的是小区名
      val SimilarCommunity =
        GetAllCommunity.map(desc =>(desc,Utils.GetDistance(target(5).toDouble,
          target(6).toDouble,desc.split(",")(5).toDouble,
          desc.split(",")(6).toDouble))).filter(c =>(!c._1.split(",")(0).
          equals(target(0)))&&c._2<m_Setting("测试系数").maxDistance)
      //下面avmModel
      val avmModel = SimilarCommunity.map(line => {
        val arrays = line._1.split(",")
        val community = new Community(arrays(0),arrays(1),arrays(2),arrays(3),arrays(4).toDouble,arrays(5).toDouble,BigDecimal(arrays(6)),BigDecimal(arrays(7)),BigDecimal(arrays(8)),BigDecimal(arrays(9)),BigDecimal(arrays(10)),BigDecimal(arrays(11)),BigDecimal(arrays(12)),BigDecimal(arrays(13)),BigDecimal(arrays(14)),BigDecimal(arrays(15)),BigDecimal(arrays(16)),BigDecimal(arrays(17)),BigDecimal(arrays(18)),BigDecimal(arrays(19)),BigDecimal(arrays(20)),BigDecimal(arrays(21)),BigDecimal(arrays(22)),BigDecimal(arrays(23)),BigDecimal(arrays(24)),null)
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


    def process(pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
                pLocation_Longitude:Double=0,pLocation_Latitude:Double=0)={

      val  targetCommunity = AllCommunityBroadcast.value.filter(line =>(line._1.CommunityName.equals(pcommunityName))).take(1)
//初始化……………………………………………………………………………………………………
      var pfaceToBargainList :List[((Double,Bargain,BigDecimal))] =Nil
      if(!targetCommunity.isEmpty){
        //其实只有一个
        val  bargainList = AllBargainMapListBroadCast.value.filter(line =>
          line._1.equals(targetCommunity.head._1.CommunityID)).take(1).toList

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

//          weight = byg._1 * Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-byg._2.bargainTime.getYear)*12 + (c.get(Calendar.MONTH) - byg._2.bargainTime.getMonth)))*1.0/maxMonth,
//            timePowerBargain)

          (weight,byg._2)
        }).filter(_._1 !=0 )
      // 根据面积开始过滤
      val square_adjust_coefficientList =settingBroadCast.value("测试系数").squareAdjustCoefficient.toList.sorted

        val square_adjust_coefficientBargain=  timeBargain.map(tb =>{

          val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
         // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
          val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
          if(!square_adjust_List.isEmpty){

          val square_adjust_weight = square_adjust_List.map(line => line match{
            case (x:Double,y:Double) => y
            case _ => 0
          }).head
            weight = tb._1*square_adjust_weight
          }else{
            weight = tb._1 *0
          }
          (weight,tb._2)
        }).filter(_._1 != 0)

        //朝向过滤开始,现在其实没有做，留个口

       val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
          weight = line._1*1
         //(权重，bargain，bargainPrice)
          (weight,line._2,line._2.bargainPrice)
        }).filter(_._1 > 0)
        pfaceToBargainList = pfaceToBargain.toList
//        if(pfaceToBargain.size > 5){
//           pfaceToBargain.toList
//
//        }
    }

//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     var pfaceToBargainList2222 :List[((Double,Bargain,BigDecimal))] =Nil
      if(true){
      var similar:Double= settingBroadCast.value("测试系数").diffrentCommunity
      val similarCommunityListReal = targetCommunity.isEmpty match {
        case true => GetSimilarCommunity(pLocation_Longitude,pLocation_Latitude).collect()
        case false =>  targetCommunity.head._2.collect().filter(scl =>
          Math.abs((targetCommunity.head._1.FeatureSoilRankValue - scl.Community.FeatureSoilRankValue).toDouble) <= BigDecimal(threadTarget._2.SoilRank)&&
          Math.abs((targetCommunity.head._1.FeatureBusCountValue - scl.Community.FeatureBusCountValue).toDouble) <= BigDecimal(threadTarget._2.BusCount)&&
          Math.abs((targetCommunity.head._1.FeatureParkValue - scl.Community.FeatureParkValue).toDouble) <= BigDecimal(threadTarget._2.Park)&&
          Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
          Math.abs((targetCommunity.head._1.FeatureTrafficControlValue - scl.Community.FeatureTrafficControlValue).toDouble) <= BigDecimal(threadTarget._2.TrafficControl)&&
          Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
          Math.abs((targetCommunity.head._1.FeatureGoodFactorValue - scl.Community.FeatureGoodFactorValue).toDouble) <= BigDecimal(threadTarget._2.GoodFactor)&&
          Math.abs((targetCommunity.head._1.FeatureBadFactorValue - scl.Community.FeatureBadFactorValue).toDouble) <= BigDecimal(threadTarget._2.BadFactor)&&
          Math.abs((targetCommunity.head._1.FeatureScopeValue - scl.Community.FeatureScopeValue).toDouble) <= BigDecimal(threadTarget._2.Scope)&&
          Math.abs((targetCommunity.head._1.FeatureBuildYearValue - scl.Community.FeatureBuildYearValue).toDouble) <= BigDecimal(threadTarget._2.BuildYear)&&
          Math.abs((targetCommunity.head._1.FeatureHeatingValue - scl.Community.FeatureHeatingValue).toDouble) <= BigDecimal(threadTarget._2.Heating)&&
          Math.abs((targetCommunity.head._1.FeatureIsSchoolValue - scl.Community.FeatureIsSchoolValue).toDouble) <= BigDecimal(threadTarget._2.IsSchool)&&
          Math.abs((targetCommunity.head._1.FeatureStyleValue - scl.Community.FeatureStyleValue).toDouble) <= BigDecimal(threadTarget._2.Style)&&
          Math.abs((targetCommunity.head._1.FeaturePropertyLevelValue - scl.Community.FeaturePropertyLevelValue).toDouble) <= BigDecimal(threadTarget._2.PropertyLevel)&&
          Math.abs((targetCommunity.head._1.FeatureEnvironmentValue - scl.Community.FeatureEnvironmentValue).toDouble) <= BigDecimal(threadTarget._2.Environment)&&
          Math.abs((targetCommunity.head._1.FeatureDensityValue - scl.Community.FeatureDensityValue).toDouble) <= BigDecimal(threadTarget._2.Density)&&
          Math.abs((targetCommunity.head._1.FeatureFARValue - scl.Community.FeatureFARValue).toDouble) <= BigDecimal(threadTarget._2.Far)&&
          Math.abs((targetCommunity.head._1.FeatureDistanceFromCenterValue - scl.Community.FeatureDistanceFromCenterValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromCenter)&&
          Math.abs((targetCommunity.head._1.FeatureDistanceFromTradingValue - scl.Community.FeatureDistanceFromTradingValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromTrading)&&
          Math.abs((targetCommunity.head._1.FeatureDistanceFromLandScapeValue - scl.Community.FeatureDistanceFromLandScapeValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromLandScape))
      }


      //如果同一个小区，权值为1，否则为传入参数,可动态调整
      similarCommunityListReal.map(scl => {
        similar = similar * Math.pow((settingBroadCast.value("测试系数").maxDistance + 1 - scl.Distance) / settingBroadCast.value("测试系数").maxDistance, settingBroadCast.value("测试系数").distancePower);
//        GetBargain(scl.Community.CommunityID).map(line =>{
//          val avmBargain = AVMBargain()
//          avmBargain.adjustPrice =line.bargainPrice
//          avmBargain.Case = line
//          avmBargain.Weight =similar
//        })
         val  bargainList2 =  GetBargain(scl.Community.CommunityID)
//*********************************************************************************************************
        val mineCoummunityBargain2 = bargainList2.filter(line => (new Date().
          getTime-line.bargainTime.getTime)>= settingBroadCast.value("测试系数").maxMonth*3600).filter(
            (line => (line.totalFloor <= threadTarget._2.CommunityStyle
              ||pTotalFloor>threadTarget._2.CommunityStyle)
              &&(pTotalFloor<=threadTarget._2.CommunityStyle||
              line.totalFloor>threadTarget._2.CommunityStyle))
          )

        //(1,(1,a))楼层分段过滤开始**********
        val flRuleList = settingBroadCast.value("测试系数").floorRule.toList

        val flRulePoint_KeyVal2 = flRuleList.filter(flr =>pfloor >= flr._2.small&&pfloor<flr._2.big)

        val flRulePoint_Key2 = flRulePoint_KeyVal2.map(line => line match{
          case (x:Int,y :Section) => x
          case _ => 0
        }).head

        var weight:Double =1
        //下面的这个是个Map
        val flCoefficientList = settingBroadCast.value("测试系数").floorCoefficient
        //楼层分段过滤开始
        val bargainFloorRule2 = mineCoummunityBargain2.map(cb =>{
          val flr_KeyVal=  flRuleList.filter(frl=>
            cb.totalFloor>=frl._2.small&&cb.totalFloor<frl._2.big)
          val targetfloorIntKey = flr_KeyVal.isEmpty match {
            case true => 0
            case false => flr_KeyVal.head._1
          }

          weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key2 - targetfloorIntKey)).getOrElse(0.0)
          (weight,cb)
        }).filter(_._1 != 0)

        //面积分段过滤开始
        val squareList = settingBroadCast.value("测试系数").squareRule.toList

        val pointsquare_KeyVal2= squareList.filter(sL =>
          psquare >= sL._2.small&&psquare<sL._2.big )
        //界面输入的值计算
        val pointSqureInt2 = pointsquare_KeyVal2.map(line => line match{
          case (x:Int,y: Section) => x
          //这个值是默认值
          case _ =>0
        }).head

        val bargainSquare2 =bargainFloorRule2.map(bfr =>{
          val slTarget_KeyVal = squareList.filter(sL => bfr._2.square>= sL._2.small &&
            bfr._2.square < sL._2.big)

          val slTarget_KeyInt =  slTarget_KeyVal.map(line => line match{
            case(x:Int,y:Section) => x
            case _ => 0
          }).head
          //因为bfr._1是double的所以默认是0.0
          weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt2 - slTarget_KeyInt)).getOrElse(0.0)
          (weight,bfr._2)
        }).filter(_._1 != 0)

        //楼栋

        //建成年份开始过滤, 输入是经过面积过滤后的
        var PointbuildYear =0
        var tagertbuildYear = 0
        val buildYearCofficent = settingBroadCast.value("测试系数").buildYearCoefficient
        val buildYearBargain2 = bargainSquare2.map(bgs => {
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
        val timeBargain2 = buildYearBargain2.map(byg => {
          //这块会有一些问题，主要看你传入的日期是什么格式的
//          weight = byg._1 * Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-byg._2.bargainTime.getYear)*12 + (c.get(Calendar.MONTH) - byg._2.bargainTime.getMonth)))*1.0/maxMonth,
//            timePowerBargain)
          (weight,byg._2)
        }).filter(_._1 !=0 )
        // 根据面积开始过滤
        val square_adjust_coefficientList =settingBroadCast.value("测试系数").squareAdjustCoefficient.toList.sorted

        val square_adjust_coefficientBargain2=  timeBargain2.map(tb =>{

          val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
          // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
          val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
          if(!square_adjust_List.isEmpty){

            val square_adjust_weight = square_adjust_List.map(line => line match{
              case (x:Double,y:Double) => y
              case _ => 0
            }).head
            weight = tb._1*square_adjust_weight
          }else{
            weight = tb._1 *0
          }
          (weight,tb._2)
        }).filter(_._1 != 0)

        //朝向过滤开始,现在其实没有做，留个口

        val pfaceToBargain2 =  square_adjust_coefficientBargain2.map(line => {
          weight = line._1*1
          (weight,line._2,line._2.bargainPrice)
        }).filter(_._1 > 0)
        pfaceToBargainList2222 = pfaceToBargain2.toList

      })



  }
      //两个最终结果进行合并
 val pfaceToBargainfinalList = pfaceToBargainList2222++pfaceToBargainList

      //留一个口价格
      var resultprice =Result()
      resultprice.price=0
      resultprice.list=null
      //&&&&要和和
      val date_Cha = new Date().getTime-3600*24*90
      //按时间进行降序排序
      val newvalueList = beijingIndexList.filter(bjsi =>
        bjsi.dateTime.getTime >= date_Cha
      ).sortBy(x => - x.dateTime.getTime).take(1).toList


      val newValue =  newvalueList.isEmpty match
      {
        case true => 0
        case false => newvalueList.head.price.toDouble
      }


      var fianlPriceAnd5house = null
     if(!pfaceToBargainfinalList.isEmpty) {
       // case true => List(0,new Bargain("0","0",0.0,0,0,"0",new Date(),BigDecimal(0)),BigDecimal(0))
      //  case false =>pfaceToBargainfinalList.map(line =>{
      val pfaceToBargainfinalList22 =  pfaceToBargainfinalList.map(line =>{
          val oldvalueList = beijingIndexList.filter(index =>
            index.dateTime.getYear == line._2.bargainTime.getYear&&
              index.dateTime.getMonth ==line._2.bargainTime.getMonth
          ).take(1).toList
          val oldvalue = oldvalueList.isEmpty match{
            case true => 0
            case false => oldvalueList.head.price.toDouble
          }

          (line._1,line._2,line._2.bargainPrice * newValue / oldvalue)

        }).filter(ptb => ptb._2.bargainTime.getTime+6*30*3600 < new Date().getTime ).sortBy(x => - x._1).take(5).toList
       //这就是最终值
        if(pfaceToBargainfinalList22.size >4){
          var avmBagainFianlReault = AVMBargain()
          var sumWeigth=0.0
          var sumadjust_Price=0.0
          var list5FianlBargain:List[AVMBargain] =List()
          pfaceToBargainfinalList22.map(x =>{

            sumWeigth +=x._1

            avmBagainFianlReault.adjustPrice =x._3
            avmBagainFianlReault.Case = x._2
            avmBagainFianlReault.Weight = x._1
            list5FianlBargain = List(avmBagainFianlReault) ++ list5FianlBargain
            // rst.Price = Math.Ceiling(cases.Sum(m => m.AdjustPrice * (decimal)m.Weight) / (decimal)sumWeight);
          })
          pfaceToBargainfinalList22.map(x =>{
            sumadjust_Price +=(x._3.toDouble* x._1 /sumWeigth)
          })
          var sumadjust_Price11 = sumadjust_Price
          //下面是最终的价钱
         val priceFinal333 =  Math.ceil(sumadjust_Price11)


          resultprice.price = priceFinal333
          resultprice.list =list5FianlBargain
          resultprice

        }
      }else{
       resultprice
     }

    }













    def  GetSimilarCommunity( pLongitude:Double, pLatitude:Double)=
    {
       AllCommunityBroadcast.value.map(line => {
         //key value对，其中key是Community，value是具体的距离
        (line._1,Utils.GetDistance(pLatitude,pLongitude,
          line._1.LocationLatitude,line._1.LocationLongitude))
      }).filter(_._2 < settingBroadCast.value("测试系数").maxDistance).map(line2 => {
          val avmc = AVMCommunity()
         avmc.Community = line2._1
         avmc.Distance =line2._2
         avmc
       })
    }


    def   GetBargain(pCommunityID:String)=
    {
      AllBargainMapList.filter(abml => abml._1.equals(pCommunityID)).collect().head._2.toList

    }
}
}


