package cn.com.gxdgroup.dataplatform.avm.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.rdd.RDD
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import cn.com.gxdgroup.dataplatform.avm.model.Setting
import cn.com.gxdgroup.dataplatform.avm.model.Index
import cn.com.gxdgroup.dataplatform.avm.model.Section
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ThinkPad on 14-5-16.
 */
object AVMUtils {
  //向列表中添加元素
  def addItem(obj:Any):List[Any]={
    obj::Nil
  }
  // 计算距离
  def rad(d:Double):Double ={
    d * Math.PI / 180.0;
  }

  val  Earth_radius:Double =6378137

  def GetDistance(lat1:Double,lng1:Double,lat2:Double,lng2:Double):Double ={

    val radLat1:Double = rad(lat1)
    val radLat2:Double = rad(lat2)
    val a = radLat1 - radLat2
    val b = rad(lng1) - rad(lng2)
    val s:Double = 2 * math.asin((math.sqrt(math.pow(math.sin(a / 2), 2) +math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2))))
    val s1 = s * Earth_radius
    math.round(s1 * 10000) / 10000

  }


  def StringToDate(str:String):Date ={
    val format = new SimpleDateFormat("yyyy-MM-dd");

    val  date = format.parse(str);
    date

  }
  def StringToDate2(str:String):Date ={
    val format = new SimpleDateFormat("yyyy/MM/dd");

    val  date = format.parse(str);
    date

  }
/*
  //初始化
  def initAll(sc:SparkContext,args:Array[String]) ={
    // val GetAllIndex = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](args(0))
    val GetAllIndex = sc.textFile(args(0))


    //    GetAllIndex.mapPartitions{iter =>
    //      println("11111111111111111111111111111111111111111111111111111111111111111")
    //      iter.map(x => println())}
    val GetAllThreshold = sc.textFile(args(1))
    val GetAllCommunity = sc.textFile(args(2))
    //val GetAllCommunity = sc.parallelize(GetAllCommunity3.toArray())
    //现在1024效果最后一共25s
    val GetAllCommunity16 = sc.textFile(args(2),args(4).toInt)

    val GetAllBargainTextFile = sc.textFile(args(3))

    //其中key是城市，val是城市、价格 、时间
    //Index初始化
    println("\n\n\n")
    println("GetAllIndex:" + GetAllIndex.collect().toList.mkString("---") )
    println("\n\n\n")
    val AllIndexMapList= GetAllIndex.map(line => {
      //先假设是逗号分隔
      val IndexDetail_Arrays = line.split(",")
      val index = Index(IndexDetail_Arrays(0).toInt,
        IndexDetail_Arrays(1),
        BigDecimal(IndexDetail_Arrays(2)),
        AVMUtils.StringToDate(IndexDetail_Arrays(3))
      )
      val cityName = IndexDetail_Arrays(1)
      (cityName,index)
    }).groupByKey().cache()
    val IndexBroadcast =  sc.broadcast(AllIndexMapList).value


    //Threshold初始化
    //AllThreshold存为key，val的RDD，其中key是阈值系数名称，val是一行值，阈值系数名称, 楼盘类型匹配阈值 , 土地等级匹配阈值 , 公交数量匹配阈值 , 停车场匹配阈值, 周边配套匹配阈值, 交通管制匹配阈值,
    // 有利因素匹配阈值 , 不利因素匹配阈值 , 楼盘规模匹配阈值 , 建成年份匹配阈值 , 供热情况匹配阈值, 学区匹配阈值 , 楼盘风格匹配阈值, 物业等级匹配阈值 ,绿化率匹配阈值  , 建筑密度匹配阈值 , 容积率匹配阈值 , 距市中心距离匹配阈值, 距商服中心距离匹配阈值
    val thresholdArray= GetAllThreshold.map(line => {
      //先假设是逗号分隔
      val tt = line.split(",")
      //此处还需要修改,其中tt(1)代表的是阈值的名
      (tt(1),  Threshold(tt(0).toInt,tt(1),tt(2).toInt,
        tt(3).toDouble,
        tt(4).toDouble,
        tt(5).toDouble,
        tt(6).toDouble,
        tt(7).toDouble,
        tt(8).toDouble,
        tt(9).toDouble,
        tt(10).toDouble,
        tt(11).toDouble,
        tt(12).toDouble,
        tt(13).toDouble,
        tt(14).toDouble,
        tt(15).toDouble,
        tt(16).toDouble,
        tt(17).toDouble,
        tt(18).toDouble,
        tt(19).toDouble,
        tt(20).toDouble,
        tt(21).toDouble))
    }).cache()
    val thresholdBroadcast = sc.broadcast(thresholdArray).value



    //SETTING初始化
    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))

    val settingBroadcast = sc.broadcast(m_Setting).value


    println("GetAllCommunity111:"+GetAllCommunity.count())

    //  Community初始化



    //  GetAllCommunity***********
    val AllCommunityTo_Array = GetAllCommunity.toArray()
    val AllCommunity=GetAllCommunity16.map(line =>{
      //先假设是逗号分隔
      val target = line.split("\t")
      println("target(7):"+target.mkString("|"))
      var community= new Community(target(0),
        target(1),
        target(2),
        target(3),
        target(4).toDouble,
        target(5).toDouble,
        BigDecimal(target(6)),
        BigDecimal(target(7)),
        BigDecimal(target(8)),
        BigDecimal(target(9)),
        BigDecimal(target(10)),
        BigDecimal(target(11)),
        BigDecimal(target(12)),
        BigDecimal(target(13)),
        BigDecimal(target(14)),
        BigDecimal(target(15)),
        BigDecimal(target(16)),
        BigDecimal(target(17)),
        BigDecimal(target(18)),
        BigDecimal(target(19)),
        BigDecimal(target(20)),
        BigDecimal(target(21)),
        BigDecimal(target(22)),
        BigDecimal(target(23)),
        BigDecimal(target(24)),null)
      //其中4,5代表的是经纬度的数据数组下标到时候还需要改,0下标代表的是小区名
      val SimilarCommunity =
        AllCommunityTo_Array.map(desc =>(desc,

          AVMUtils.GetDistance(target(5).toDouble,
            target(4).toDouble,
            desc.split("\t")(5).toDouble,
            desc.split("\t")(4).toDouble)))
          .filter(c =>(!c._1.split("\t")(0).
          ==(target(0)))&&c._2<m_Setting("测试系数").maxDistance)
      //下面avmModel
      val avmModel = SimilarCommunity.map(line => {
        val arrays = line._1.split("\t")
        val community = new Community(arrays(0),
          arrays(1),
          arrays(2),
          arrays(3),
          arrays(4).toDouble,
          arrays(5).toDouble,
          BigDecimal(arrays(6)),
          BigDecimal(arrays(7)),
          BigDecimal(arrays(8)),
          BigDecimal(arrays(9)),
          BigDecimal(arrays(10)),
          BigDecimal(arrays(11)),
          BigDecimal(arrays(12)),
          BigDecimal(arrays(13)),
          BigDecimal(arrays(14)),
          BigDecimal(arrays(15)),
          BigDecimal(arrays(16)),
          BigDecimal(arrays(17)),
          BigDecimal(arrays(18)),
          BigDecimal(arrays(19)),
          BigDecimal(arrays(20)),
          BigDecimal(arrays(21)),
          BigDecimal(arrays(22)),
          BigDecimal(arrays(23)),
          BigDecimal(arrays(24)),null)
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
    //val AllCommunityBroadcast = sc.broadcast(AllCommunity)

    val AllCommunityBroadcast = sc.broadcast(AllCommunity).value


    //Bargain初始化
    //取得所有Bargain，将其存在Map中其中key是小区名，value是list由所有的房子组成,注意要定义成var的
    val AllBargainMapList = GetAllBargainTextFile.map(line => {
      val BargainDetail_Arrays = line.split("\t")
      //这个之后还需要改，不确定具体的字段对应
      val bargain = Bargain(
        BargainDetail_Arrays(0),
        BargainDetail_Arrays(1),
        BargainDetail_Arrays(2).toDouble,
        BargainDetail_Arrays(4).toInt,
        BargainDetail_Arrays(5).toInt,
        BargainDetail_Arrays(8),
        StringToDate2(BargainDetail_Arrays(9)),
        BargainDetail_Arrays(10).toDouble)
      val communityID = BargainDetail_Arrays(1)
      (communityID,bargain)
    }).groupByKey().cache()
    val AllBargainMapListBroadCast = sc.broadcast(AllBargainMapList).value

    (IndexBroadcast,thresholdBroadcast,settingBroadcast,AllCommunityBroadcast,AllBargainMapListBroadCast)
  }

  //封装方法,返回结果是(权重，bargain，bargainPrice)
  //初始化的时候直接输出


  def calculateModel(bargainList:Seq[Bargain],thresholds:RDD[(String,Threshold)],settingBroadcast:Map[String,Setting],pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
                     pLocation_Longitude:Double=0,pLocation_Latitude:Double=0):List[(Double,Bargain,BigDecimal)]={

    val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).first()
    val mineCoummunityBargain = bargainList.filter(line => (new Date().
      getTime-line.bargainTime.getTime)>= settingBroadcast("测试系数").maxMonth*3600).filter(
        (line => (line.totalFloor <= threadTarget._2.CommunityStyle
          ||pTotalFloor>threadTarget._2.CommunityStyle)
          &&(pTotalFloor<=threadTarget._2.CommunityStyle||
          line.totalFloor>threadTarget._2.CommunityStyle))
      )
    //println("\n\n************************************************\nfloorRule\n************************************")
    val flRuleList = settingBroadcast("测试系数").floorRule.toList
  //  println("\n\n************************************************\nfloorRuleAFTER\n************************************")
    val flRulePoint_KeyVal = flRuleList.filter(flr =>
      pfloor >= flr._2.small
        &&pfloor<flr._2.big)

    val flRulePoint_Key = flRulePoint_KeyVal.map(line => line match{
      case (x:Int,y :Section) => x
      case _ => 0
    }).head

    var weight:Double =1
    //下面的这个是个Map
    val flCoefficientList = settingBroadcast("测试系数").floorCoefficient
    //楼层分段过滤开始
    val bargainFloorRule = mineCoummunityBargain.map(cb =>{
      weight=1
      val flr_KeyVal=  flRuleList.filter(frl=>
        cb.totalFloor>=frl._2.small&&
          cb.totalFloor<frl._2.big)
      val targetfloorIntKey = flr_KeyVal.isEmpty match {
        case true => 0
        case false => flr_KeyVal.head._1
      }

      weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
      (weight,cb)
    }).filter(_._1 != 0.0)

    //面积分段过滤开始
    val squareList = settingBroadcast("测试系数").squareRule.toList

    val pointsquare_KeyVal= squareList.filter(sL =>
      psquare >= sL._2.small&&
        psquare<sL._2.big )
    //界面输入的值计算
    val pointSqureInt = pointsquare_KeyVal.map(line => line match{
      case (x:Int,y: Section) => x
      //这个值是默认值
      case _ =>0
    }).head

    val bargainSquare =bargainFloorRule.map(bfr =>{
      val slTarget_KeyVal = squareList.filter(sL =>
        bfr._2.square>= sL._2.small &&
          bfr._2.square < sL._2.big)
      val slTarget_KeyInt =  slTarget_KeyVal.map(line =>
        line match{
          case(x:Int,y:Section) => x
          case _ => 0
        }).head
      //因为bfr._1是double的所以默认是0.0
      weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)
      (weight,bfr._2)
    }).filter(_._1 != 0.0)

    //楼栋

    //建成年份开始过滤, 输入是经过面积过滤后的,bargainSquare也应该进行判断是否为空
    var PointbuildYear =0
    var tagertbuildYear = 0
    val buildYearCofficent = settingBroadcast("测试系数").buildYearCoefficient

    val buildYearBargain = bargainSquare.map(bgs => {

      if(StringUtils.isNumeric(pbuildYear)&& StringUtils.isNumeric(bgs._2.BuildYear)){
        PointbuildYear = pbuildYear.toInt
        tagertbuildYear = bgs._2.BuildYear.toInt

        val buildCha = Math.abs(PointbuildYear-tagertbuildYear)
        val buildKey = buildYearCofficent.get(buildCha).getOrElse(0.0)
        weight = bgs._1 * buildKey
        (weight,bgs._2)
      }else{
        (0.0,bgs._2)
      }
    }).filter(_._1 != 0.0)




    //根据挂牌时间开始过滤，这里的buildYearBargain也应该判断是否为空
    val maxMonth = settingBroadcast("测试系数").maxMonth
    val timePowerBargain =settingBroadcast("测试系数").bargainTimePower
    val  c = Calendar.getInstance()
    val  c2 = Calendar.getInstance()
    val timeBargain = buildYearBargain.map(byg => {
      //这块会有一些问题，主要看你传入的日期是什么格式的
      c2.setTime(byg._2.bargainTime)
      weight =  byg._1*Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-c2.get(Calendar.YEAR))*12 +
        (c.get(Calendar.MONTH) -c2.get(Calendar.MONTH))))*1.0/maxMonth,
        timePowerBargain)

      (weight,byg._2)
    }).filter(_._1 !=0.0 )


    // 根据面积开始过滤
    val square_adjust_coefficientList =settingBroadcast("测试系数").squareAdjustCoefficient.toList.sorted

    val square_adjust_coefficientBargain=  timeBargain.map(tb =>{

      val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
      // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
      val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
      if(!square_adjust_List.isEmpty){

        val square_adjust_weight = square_adjust_List.map(line => line match{
          case (x:Double,y:Double) => y
          case _ => 0.0
        }).head
        weight = tb._1*square_adjust_weight
      }else{
        weight = tb._1 *0.0
      }
      (weight,tb._2)
    }).filter(_._1 != 0.0)


    //朝向过滤开始,现在其实没有做，留个口

    val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
      weight = line._1*1
      //(权重，bargain，bargainPrice)
      println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice)
      (weight,line._2,line._2.bargainPrice)

    }).filter(_._1 > 0.0)


    pfaceToBargain.toList

  }




  def mapCalculateModel(bargainList:Seq[Bargain],thresholds:Array[(String,Threshold)],settingBroadcast:Map[String,Setting],pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
                     pLocation_Longitude:Double=0,pLocation_Latitude:Double=0):List[(Double,Bargain,BigDecimal)]={

    val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).head
    val mineCoummunityBargain = bargainList.filter(line => (new Date().
      getTime-line.bargainTime.getTime)>= settingBroadcast("测试系数").maxMonth*3600).filter(
        (line => (line.totalFloor <= threadTarget._2.CommunityStyle
          ||pTotalFloor>threadTarget._2.CommunityStyle)
          &&(pTotalFloor<=threadTarget._2.CommunityStyle||
          line.totalFloor>threadTarget._2.CommunityStyle))
      )
    println("\n\n************************************************\nfloorRule\n************************************")
    val flRuleList = settingBroadcast("测试系数").floorRule.toList
    println("\n\n************************************************\nfloorRuleAFTER\n************************************")
    val flRulePoint_KeyVal = flRuleList.filter(flr =>
      pfloor >= flr._2.small
        &&pfloor<flr._2.big)

    val flRulePoint_Key = flRulePoint_KeyVal.map(line => line match{
      case (x:Int,y :Section) => x
      case _ => 0
    }).head

    var weight:Double =1
    //下面的这个是个Map
    val flCoefficientList = settingBroadcast("测试系数").floorCoefficient
    //楼层分段过滤开始
    val bargainFloorRule = mineCoummunityBargain.map(cb =>{
      weight=1
      val flr_KeyVal=  flRuleList.filter(frl=>
        cb.totalFloor>=frl._2.small&&
          cb.totalFloor<frl._2.big)
      val targetfloorIntKey = flr_KeyVal.isEmpty match {
        case true => 0
        case false => flr_KeyVal.head._1
      }

      weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
      (weight,cb)
    }).filter(_._1 != 0.0)

    //面积分段过滤开始
    val squareList = settingBroadcast("测试系数").squareRule.toList

    val pointsquare_KeyVal= squareList.filter(sL =>
      psquare >= sL._2.small&&
        psquare<sL._2.big )
    //界面输入的值计算
    val pointSqureInt = pointsquare_KeyVal.map(line => line match{
      case (x:Int,y: Section) => x
      //这个值是默认值
      case _ =>0
    }).head

    val bargainSquare =bargainFloorRule.map(bfr =>{
      val slTarget_KeyVal = squareList.filter(sL =>
        bfr._2.square>= sL._2.small &&
          bfr._2.square < sL._2.big)
      val slTarget_KeyInt =  slTarget_KeyVal.map(line =>
        line match{
          case(x:Int,y:Section) => x
          case _ => 0
        }).head
      //因为bfr._1是double的所以默认是0.0
      weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)
      (weight,bfr._2)
    }).filter(_._1 != 0.0)

    //楼栋

    //建成年份开始过滤, 输入是经过面积过滤后的,bargainSquare也应该进行判断是否为空
    var PointbuildYear =0
    var tagertbuildYear = 0
    val buildYearCofficent = settingBroadcast("测试系数").buildYearCoefficient

    val buildYearBargain = bargainSquare.map(bgs => {

      if(StringUtils.isNumeric(pbuildYear)&& StringUtils.isNumeric(bgs._2.BuildYear)){
        PointbuildYear = pbuildYear.toInt
        tagertbuildYear = bgs._2.BuildYear.toInt

        val buildCha = Math.abs(PointbuildYear-tagertbuildYear)
        val buildKey = buildYearCofficent.get(buildCha).getOrElse(0.0)
        weight = bgs._1 * buildKey
        (weight,bgs._2)
      }else{
        (0.0,bgs._2)
      }
    }).filter(_._1 != 0.0)




    //根据挂牌时间开始过滤，这里的buildYearBargain也应该判断是否为空
    val maxMonth = settingBroadcast("测试系数").maxMonth
    val timePowerBargain =settingBroadcast("测试系数").bargainTimePower
    val  c = Calendar.getInstance()
    val  c2 = Calendar.getInstance()
    val timeBargain = buildYearBargain.map(byg => {
      //这块会有一些问题，主要看你传入的日期是什么格式的
      c2.setTime(byg._2.bargainTime)
      weight =  byg._1*Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-c2.get(Calendar.YEAR))*12 +
        (c.get(Calendar.MONTH) -c2.get(Calendar.MONTH))))*1.0/maxMonth,
        timePowerBargain)

      (weight,byg._2)
    }).filter(_._1 !=0.0 )


    // 根据面积开始过滤
    val square_adjust_coefficientList =settingBroadcast("测试系数").squareAdjustCoefficient.toList.sorted

    val square_adjust_coefficientBargain=  timeBargain.map(tb =>{

      val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
      // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
      val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
      if(!square_adjust_List.isEmpty){

        val square_adjust_weight = square_adjust_List.map(line => line match{
          case (x:Double,y:Double) => y
          case _ => 0.0
        }).head
        weight = tb._1*square_adjust_weight
      }else{
        weight = tb._1 *0.0
      }
      (weight,tb._2)
    }).filter(_._1 != 0.0)


    //朝向过滤开始,现在其实没有做，留个口

    val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
      weight = line._1*1
      //(权重，bargain，bargainPrice)
      println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice)
      (weight,line._2,line._2.bargainPrice)

    }).filter(_._1 > 0.0)


    pfaceToBargain.toList

  }

  //不同小区根据经纬度求相似小区
  def  GetSimilarCommunity( sc:SparkContext,communities:RDD[(Community,Array[AVMCommunity])],
                            setting:Map[String,Setting],
                            pLongitude:Double,
                            pLatitude:Double): RDD[AVMCommunity] =
  {
    println("\n\n************************************************\nGetSimilarCommunity\n************************************")
    communities.map(line => {
      //key value对，其中key是Community，value是具体的距离

      (line._1,AVMUtils.GetDistance(pLatitude,pLongitude,
        line._1.LocationLatitude,line._1.LocationLongitude))
    }).filter(_._2 < setting("测试系数").maxDistance).map(line2 => {
      val avmc = AVMCommunity()
      avmc.Community = line2._1
      avmc.Distance =line2._2
      avmc
    })
  }

  def   GetBargain(bargains:RDD[(String,Seq[Bargain])],pCommunityID:String)=
  {
    if(!bargains.filter(abml => abml._1.equals(pCommunityID)).collect().isEmpty){


      bargains.filter(abml => abml._1.equals(pCommunityID)).collect().head._2.toList
    }else{
      val a :Seq[Bargain] = List()
      a
    }


  }


  def mapGetBargain[A <:Map[String,Seq[Bargain]]](bargains: A,pCommunityID:String)=
  {
     val listBargain:Seq[Bargain] = bargains.get(pCommunityID).getOrElse(null)
    if(listBargain != null){

      listBargain
    }else{
      val a :Seq[Bargain] = List()
      a
    }
  }
  //    var SimilarCommunityList:List[String] =Nil

  def xyz(bargainList:Seq[Bargain],thresholds:Array[(String,Threshold)],settingBroadcast:Map[String,Setting],
          pcommunityName:String,pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
          pLocation_Longitude:Double=0,pLocation_Latitude:Double=0):List[(Double,Bargain,BigDecimal)]={

    val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).head
    val mineCoummunityBargain = bargainList.filter(line => (new Date().
      getTime-line.bargainTime.getTime)>= settingBroadcast("测试系数").maxMonth*3600).filter(
        (line => (line.totalFloor <= threadTarget._2.CommunityStyle
          ||pTotalFloor>threadTarget._2.CommunityStyle)
          &&(pTotalFloor<=threadTarget._2.CommunityStyle||
          line.totalFloor>threadTarget._2.CommunityStyle))
      )
    println("\n\n************************************************\nfloorRule\n************************************")
    val flRuleList = settingBroadcast("测试系数").floorRule.toList
    println("\n\n************************************************\nfloorRuleAFTER\n************************************")
    val flRulePoint_KeyVal = flRuleList.filter(flr =>
      pfloor >= flr._2.small
        &&pfloor<flr._2.big)

    val flRulePoint_Key = flRulePoint_KeyVal.map(line => line match{
      case (x:Int,y :Section) => x
      case _ => 0
    }).head

    var weight:Double =1
    //下面的这个是个Map
    val flCoefficientList = settingBroadcast("测试系数").floorCoefficient
    //楼层分段过滤开始
    val bargainFloorRule = mineCoummunityBargain.map(cb =>{
      weight=1
      val flr_KeyVal=  flRuleList.filter(frl=>
        cb.totalFloor>=frl._2.small&&
          cb.totalFloor<frl._2.big)
      val targetfloorIntKey = flr_KeyVal.isEmpty match {
        case true => 0
        case false => flr_KeyVal.head._1
      }

      weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
      (weight,cb)
    }).filter(_._1 != 0.0)

    //面积分段过滤开始
    val squareList = settingBroadcast("测试系数").squareRule.toList

    val pointsquare_KeyVal= squareList.filter(sL =>
      psquare >= sL._2.small&&
        psquare<sL._2.big )
    //界面输入的值计算
    val pointSqureInt = pointsquare_KeyVal.map(line => line match{
      case (x:Int,y: Section) => x
      //这个值是默认值
      case _ =>0
    }).head

    val bargainSquare =bargainFloorRule.map(bfr =>{
      val slTarget_KeyVal = squareList.filter(sL =>
        bfr._2.square>= sL._2.small &&
          bfr._2.square < sL._2.big)
      val slTarget_KeyInt =  slTarget_KeyVal.map(line =>
        line match{
          case(x:Int,y:Section) => x
          case _ => 0
        }).head
      //因为bfr._1是double的所以默认是0.0
      weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)
      (weight,bfr._2)
    }).filter(_._1 != 0.0)

    //楼栋

    //建成年份开始过滤, 输入是经过面积过滤后的,bargainSquare也应该进行判断是否为空
    var PointbuildYear =0
    var tagertbuildYear = 0
    val buildYearCofficent = settingBroadcast("测试系数").buildYearCoefficient

    val buildYearBargain = bargainSquare.map(bgs => {

      if(StringUtils.isNumeric(pbuildYear)&& StringUtils.isNumeric(bgs._2.BuildYear)){
        PointbuildYear = pbuildYear.toInt
        tagertbuildYear = bgs._2.BuildYear.toInt

        val buildCha = Math.abs(PointbuildYear-tagertbuildYear)
        val buildKey = buildYearCofficent.get(buildCha).getOrElse(0.0)
        weight = bgs._1 * buildKey
        (weight,bgs._2)
      }else{
        (0.0,bgs._2)
      }
    }).filter(_._1 != 0.0)




    //根据挂牌时间开始过滤，这里的buildYearBargain也应该判断是否为空
    val maxMonth = settingBroadcast("测试系数").maxMonth
    val timePowerBargain =settingBroadcast("测试系数").bargainTimePower
    val  c = Calendar.getInstance()
    val  c2 = Calendar.getInstance()
    val timeBargain = buildYearBargain.map(byg => {
      //这块会有一些问题，主要看你传入的日期是什么格式的
      c2.setTime(byg._2.bargainTime)
      weight =  byg._1*Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-c2.get(Calendar.YEAR))*12 +
        (c.get(Calendar.MONTH) -c2.get(Calendar.MONTH))))*1.0/maxMonth,
        timePowerBargain)

      (weight,byg._2)
    }).filter(_._1 !=0.0 )


    // 根据面积开始过滤
    val square_adjust_coefficientList =settingBroadcast("测试系数").squareAdjustCoefficient.toList.sorted

    val square_adjust_coefficientBargain=  timeBargain.map(tb =>{

      val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
      // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
      val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
      if(!square_adjust_List.isEmpty){

        val square_adjust_weight = square_adjust_List.map(line => line match{
          case (x:Double,y:Double) => y
          case _ => 0.0
        }).head
        weight = tb._1*square_adjust_weight
      }else{
        weight = tb._1 *0.0
      }
      (weight,tb._2)
    }).filter(_._1 != 0.0)


    //朝向过滤开始,现在其实没有做，留个口

    val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
      weight = line._1*1
      //(权重，bargain，bargainPrice)
      println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice)
      (weight,line._2,line._2.bargainPrice)

    }).filter(_._1 > 0.0)


    pfaceToBargain.toList

  }
 */
}