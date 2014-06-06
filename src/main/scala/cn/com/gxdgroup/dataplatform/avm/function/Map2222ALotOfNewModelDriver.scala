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

import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}

import scala.concurrent.{Await, Future}


import scala.BigDecimal
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
import scala.collection.mutable.ArrayBuffer
import redis.clients.jedis.Tuple

/**
 * Created by ThinkPad on 14-6-3.
 */
object Map2222ALotOfNewModelDriver {

  def main(args: Array[String]){

    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))


    val GetAllIndex = sc.textFile(args(0))
    val GetAllThreshold = sc.textFile(args(1))
    val GetAllCommunity = sc.textFile(args(2))
    val GetAllBargainTextFile = sc.textFile(args(3))
    val initializesimlarCommunitys = sc.textFile(args(4))
    val AlotOFBargainList=sc.textFile(args(5),args(6).toInt)
    val GetAllCommunityToArray = GetAllCommunity.toArray()

    val collectMapCommunity = GetAllCommunity.map(line =>{
      var lines = line.split("\t")
      (lines(0),line)
    }).collectAsMap()

    val collectMapinitializesSC =  initializesimlarCommunitys.map(line => {
      var lines = line.split(",")
      (lines(0),line)
    }).groupByKey().collectAsMap()



    val indexes= GetAllIndex.map(line => {
      //先假设是逗号分隔
      val IndexDetail_Arrays = line.split(",")
      val index = Index(IndexDetail_Arrays(0).toInt,
        IndexDetail_Arrays(1),
        BigDecimal(IndexDetail_Arrays(2)),
        AVMUtils.StringToDate(IndexDetail_Arrays(3))
      )
      val cityName = IndexDetail_Arrays(1)
      (cityName,index)
    }).groupByKey().toArray()
    // val indexes =  sc.broadcast(AllIndexMapList).value
    //println("indexes:"+indexes.count())
    //println("AllIndexMapList:"+AllIndexMapList.count())

    val thresholds= GetAllThreshold.map(line => {
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

    }).toArray()
    // val thresholds = sc.broadcast(thresholdArray).value


    //SETTING初始化
    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](0 ->Section(-10,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999999))
    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1000,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))

    val setting = sc.broadcast(m_Setting).value



    val bargains = GetAllBargainTextFile.map(line => {
      val BargainDetail_Arrays = line.split("\t")
      //这个之后还需要改，不确定具体的字段对应
      val bargain = Bargain(
        BargainDetail_Arrays(0),
        BargainDetail_Arrays(1),
        BargainDetail_Arrays(2).toDouble,
        BargainDetail_Arrays(4).toInt,
        BargainDetail_Arrays(5).toInt,
        BargainDetail_Arrays(8),
        AVMUtils.StringToDate2(BargainDetail_Arrays(9)),
        BargainDetail_Arrays(10).toDouble)
      val communityID = BargainDetail_Arrays(1)
      (communityID,bargain)
    }).groupByKey().collectAsMap()
    /**
    AlotOFBargainList.mapPartitions(iter =>{
      iter.map(line =>{
        println("AlotOFBargainList.mapPartitions:"+line)
       val bargainArrays =  line.split("\t")
        process(bargainArrays(1),
                 bargainArrays(4).toInt,
                 bargainArrays(5).toInt,
                 bargainArrays(2).toDouble,
                  "朝向",
                 bargainArrays(8))
      })
    }).take(100000)
      */


    //AlotOFBargainList.foreachPartition(iter =>
    /**
    AlotOFBargainList.mapPartitions(iter =>
    {

      //iter.foreach(line =>{
      iter.map(line =>{
        //println("AlotOFBargainList.mapPartitions:"+line)
        val bargainArrays =  line.split("\t")
        process(bargainArrays(1),
          bargainArrays(4).toInt,
          bargainArrays(5).toInt,
          bargainArrays(2).toDouble,
          "朝向",
          bargainArrays(8))
      })
    }

    )
      */


    var listBargainFirst:List[String]=Nil
    val lg = AlotOFBargainList.map{line =>
   //   process(line)
//      def process(bargainDetial:String,
//                  pLocation_Longitude:Double=0,pLocation_Latitude:Double=0):(String,String,String,String,String,String) = {
      val bargainDetial = line
      val pLocation_Longitude:Double=0
      val pLocation_Latitude:Double=0

        val bargainDetialArrays = bargainDetial.split("\t")
        val bargainID = bargainDetialArrays(0)
        val pcommunityID = bargainDetialArrays(1)

        val pfloor = bargainDetialArrays(4).toInt
        val pTotalFloor= bargainDetialArrays(5).toInt
        val psquare =  bargainDetialArrays(5).toInt
        val pfaceTo =bargainDetialArrays(3)
        val pbuildYear = bargainDetialArrays(8)

        val  targetCommunityList =collectMapCommunity.get(pcommunityID).getOrElse(null)

        val  targetSimilarCommunityList:Seq[String] = collectMapinitializesSC.get(pcommunityID).getOrElse(null)

        var firstResultList :List[((Double,Bargain,BigDecimal))] =Nil
        var secondToBargainList :List[((Double,Bargain,BigDecimal))] =Nil

        val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).head
        //    println("\n\n************************************************\nFirst\n************************************")
        //  println("\n\n************************************************\ntargetCommunityList\n************************************:"+targetCommunityList.size)

        val  bargainList:Seq[Bargain] = bargains.get(pcommunityID).getOrElse(null)
        if(targetCommunityList != null && bargainList != null){

          firstResultList = mapCalculateModel(bargainList,thresholds,setting,
            pcommunityID,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
        }

        //      println("\n\n************************************************\nSecond\n************************************"
        //        + firstResultList.size)
        if(firstResultList.size < 5){
          var similar:Double= setting("测试系数").diffrentCommunity
          //   println("\n\n************************************************\nFive FIVE\n************************************")
          val similarCommunityListReal:List[(String,Double)] = targetCommunityList==null match {
            // case true => GetSimilarCommunity2(GetAllCommunityToArray,setting,pLocation_Longitude,pLocation_Latitude).toList
            case true => List()
            case false =>  collectMapinitializesSC.get(pcommunityID).map{x=>
              x.toArray.filter(scl =>{

                val array42Fields = scl.split(",")
                (
                  Math.abs((array42Fields(2).toDouble- array42Fields(22).toDouble)) <= BigDecimal(threadTarget._2.SoilRank)&&
                    Math.abs((array42Fields(3).toDouble - array42Fields(23).toDouble)) <= BigDecimal(threadTarget._2.BusCount)&&
                    Math.abs((array42Fields(4).toDouble - array42Fields(24).toDouble)) <= BigDecimal(threadTarget._2.Park)&&
                    Math.abs((array42Fields(5).toDouble - array42Fields(25).toDouble)) <= BigDecimal(threadTarget._2.Amenities)&&
                    Math.abs((array42Fields(6).toDouble - array42Fields(26).toDouble)) <= BigDecimal(threadTarget._2.TrafficControl)&&
                    Math.abs((array42Fields(7).toDouble - array42Fields(27).toDouble)) <= BigDecimal(threadTarget._2.GoodFactor)&&
                    Math.abs((array42Fields(8).toDouble - array42Fields(28).toDouble)) <= BigDecimal(threadTarget._2.BadFactor)&&
                    Math.abs((array42Fields(9).toDouble - array42Fields(29).toDouble)) <= BigDecimal(threadTarget._2.Scope)&&
                    Math.abs((array42Fields(10).toDouble - array42Fields(30).toDouble)) <= BigDecimal(threadTarget._2.BuildYear)&&
                    Math.abs((array42Fields(11).toDouble - array42Fields(31).toDouble)) <= BigDecimal(threadTarget._2.Heating)&&
                    Math.abs((array42Fields(12).toDouble - array42Fields(32).toDouble)) <= BigDecimal(threadTarget._2.IsSchool)&&
                    Math.abs((array42Fields(13).toDouble - array42Fields(33).toDouble)) <= BigDecimal(threadTarget._2.Style)&&
                    Math.abs((array42Fields(14).toDouble - array42Fields(34).toDouble)) <= BigDecimal(threadTarget._2.PropertyLevel)&&
                    Math.abs((array42Fields(15).toDouble - array42Fields(35).toDouble)) <= BigDecimal(threadTarget._2.Environment)&&
                    Math.abs((array42Fields(16).toDouble - array42Fields(36).toDouble)) <= BigDecimal(threadTarget._2.Density)&&
                    Math.abs((array42Fields(17).toDouble - array42Fields(37).toDouble)) <= BigDecimal(threadTarget._2.Far)&&
                    Math.abs((array42Fields(18).toDouble - array42Fields(38).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromCenter)&&
                    Math.abs((array42Fields(19).toDouble - array42Fields(39).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromTrading)&&
                    Math.abs((array42Fields(20).toDouble - array42Fields(40).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromLandScape))
              }).map(line => {
                val array42FieldsTwo = line.split(",")
                (array42FieldsTwo(21),array42FieldsTwo(41).toDouble)
              }).toList
            }.getOrElse(List())
          }

          similarCommunityListReal.isEmpty match{
            case true => secondToBargainList =Nil
            case false =>
              //如果同一个小区，权值为1，否则为传入参数,可动态调整
              similarCommunityListReal.map(scl => {
                similar = similar * Math.pow((setting("测试系数").maxDistance + 1 - scl._2) / setting("测试系数").maxDistance, setting("测试系数").distancePower)

                val  bargainList2:Seq[Bargain] =  bargains.get(scl._1).getOrElse(null)

                if(bargainList2 != null){
                  secondToBargainList = mapCalculateModel(bargainList2,thresholds,setting,
                    pcommunityID,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
                }else{
                  secondToBargainList =Nil
                }

              })
          }

        }

        //   println("\n\n************************************************\nThird\n************************************")
        //两个最终结果进行合并
        val pfaceToBargainfinalList = firstResultList++secondToBargainList
        //  println("\n\n************************************************\nFour+\n************************************")
        //println("\\n\\n***********************************\\npfaceToBargainfinalListPrice+\\n****************:"
        //   +pfaceToBargainfinalList.size)

        //留一个口,价格
        var resultprice =Result()
        resultprice.price=0
        resultprice.list=null
        val date_Cha = new Date().getTime-3600*24*90*1000l

        val beijingIndexList = indexes.filter(aiml=> aiml._1.equals("北京")).take(1).head._2.toList
        //  println("beijingIndexList:"+beijingIndexList.head.price)
        //  println("beijingIndexListGETtime:"+beijingIndexList.last.dateTime)
        // println("date_Cha:"+date_Cha)
        //按时间进行降序排序
        val newvalueList = beijingIndexList.filter{bjsi =>

        //  println("liuliu::::::"+bjsi.dateTime.getTime)
          bjsi.dateTime.getTime >= date_Cha
        }.sortBy(x =>
          - x.dateTime.getTime).take(1).toList

        // println("newvalueList@@@@@@@@@@@@:"+newvalueList.size)
        val newValue =  newvalueList.isEmpty match
        {
          case true => 0
          case false => newvalueList.head.price.toDouble
        }

        var fianlPriceAnd5house = null
        if(!pfaceToBargainfinalList.isEmpty) {
          // case true => List(0,new Bargain("0","0",0.0,0,0,"0",new Date(),BigDecimal(0)),BigDecimal(0))
          //  case false =>pfaceToBargainfinalList.map(line =>{
          val pfaceToBargainfinalList23 =  pfaceToBargainfinalList.map(line =>{
            val oldvalueList = beijingIndexList.filter(index =>

              index.dateTime.getYear == line._2.bargainTime.getYear&&
                index.dateTime.getMonth ==line._2.bargainTime.getMonth
            ).take(1).toList
            val oldvalue = oldvalueList.isEmpty match{
              case true => 0
              case false => oldvalueList.head.price.toDouble
            }

            (line._1,line._2,line._2.bargainPrice * newValue / oldvalue)

          })

          //   println("23Size:" + pfaceToBargainfinalList23.size)
          val pfaceToBargainfinalList22 = pfaceToBargainfinalList23.filter(ptb =>
            ptb._2.bargainTime.getTime+6*30*3600 < new Date().getTime ).sortBy(x => - x._1).take(5).toList
          // println("*****:" + pfaceToBargainfinalList22(0)._3)
          //这就是最终值
          if(pfaceToBargainfinalList22.size >4){
            var avmBagainFianlReault = AVMBargain()
            var sumWeigth=0.0
            var sumadjust_Price=0.0
            var list5FianlBargain:List[AVMBargain] =List()
            pfaceToBargainfinalList22.map(x =>{

              sumWeigth +=x._1

              avmBagainFianlReault.adjustPrice =x._3
              //  println("adjustPrice: PRICE:"+x._3)
              avmBagainFianlReault.Case = x._2
              avmBagainFianlReault.Weight = x._1
              list5FianlBargain = List(avmBagainFianlReault) ++ list5FianlBargain

              // rst.Price = Math.Ceiling(cases.Sum(m => m.AdjustPrice * (decimal)m.Weight) / (decimal)sumWeight);
            })
            //  println("list5FianlBargain: SIZE:" +list5FianlBargain.size)
            pfaceToBargainfinalList22.map(x =>{
              sumadjust_Price +=(x._3.toDouble* x._1 /sumWeigth)

            })
            // println("sumadjust_Price: SUM size:" +sumadjust_Price)
            var sumadjust_Price11 = sumadjust_Price
            //下面是最终的价钱
            val priceFinal333 =  Math.ceil(sumadjust_Price11)

            resultprice.price = priceFinal333
            resultprice.list =list5FianlBargain
            resultprice
            println("price:"+resultprice.price)
            println("Object resultprice:"+resultprice.list.head.Case.id)
            val array5simially = resultprice.list.map{
              line =>
                line.Case.id+"\t"+line.Case.communityID+"\t"+line.Case.square+"\t"+"\t"+line.Case.currentFloor+"\t"+line.Case.totalFloor+"\t"+"\t"+"\t"+line.Case.BuildYear+"\t"+line.Case.bargainTime+"\t"+line.Case.bargainPrice
            }.toArray
            //由价格和本身自己批量查询的Bargain详细信息
            val finalResultString= resultprice.price+"\t"+bargainDetial
            val sim1 = bargainID+"\t" +array5simially(0)
            val sim2 = bargainID+"\t" +array5simially(1)
            val sim3 = bargainID+"\t" +array5simially(2)
            val sim4 = bargainID+"\t" +array5simially(3)
            val sim5 = bargainID+"\t" +array5simially(4)

            (finalResultString,sim1,sim2,sim3,sim4,sim5)

            // println("finalResultString*111111111111111111***************:"+finalResultString.size)
            //  sc.parallelize(finalListResultAnd5Simily).saveAsTextFile("lg/gxd/finalResult")
          }else{

            val defaultResultVal = resultprice.price+"\t"+bargainDetial

            (defaultResultVal,"","","","","")

          }

        }else{


          // println("price222:"+resultprice.price)
          //  println("Object resultprice222:"+resultprice)
          //  val finalResultDefault = pcommunityID+","+resultprice.price+","+

          //  println("finalResultString2$$$$$$$$$$$$$:"+finalResultString2)


          //    println("listparallize2222222222222222222:"+listparallize222.size)

          //sc.parallelize(finalListResultAnd5Simily222).saveAsTextFile("lg/gxd/finalResult2")


          val defaultResultVal2 = resultprice.price+"\t"+bargainDetial
          listBargainFirst = defaultResultVal2::listBargainFirst
          (defaultResultVal2,"","","","","")
        }







    }.cache()
    val lg2 = lg.map{x => x._1}
      lg2.saveAsTextFile("lg/gxd/tableOne")

    //val lg3 = lg.filter(x => x._6 != "").flatMap{
    val lg3 = lg.flatMap{
      x =>
        List(x._2,x._3,x._4,x._5,x._6)
    }
    lg3.saveAsTextFile("lg/gxd/tableTwo")





  //  println("listBargainFirst%%%%%%%%%%%%%%%%%%%%%:"+listBargainFirst.size)

    sc.parallelize(listBargainFirst).saveAsTextFile("lg/gxd/liu")


  }








  def mkdirdou(num:Int):String={
    var dou =""
    for(i <- 0 to num){
      dou = dou+"\t"
    }
    dou
  }



  def  GetSimilarCommunity2(GetAllCommunity:Array[String],
                            setting:Map[String,Setting],
                            pLongitude:Double,
                            pLatitude:Double) =
  {
    println("\n\n************************************************\nGetSimilarCommunity\n************************************")
    //得到的结果就是小区ID和相似距离
    GetAllCommunity.map(line => {
      //key value对，其中key是Community，value是具体的距离
      val arraysFields = line.split("\t")

      (line(0).toString,AVMUtils.GetDistance(pLatitude,pLongitude,
        line(5),line(4)))
    }).filter(_._2 < setting("测试系数").maxDistance)

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
    //  println("\n\n************************************************\nfloorRule\n************************************")
    val flRuleList = settingBroadcast("测试系数").floorRule.toList
    // println("\n\n************************************************\nfloorRuleAFTER\n************************************")
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
      //  println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice)
      (weight,line._2,line._2.bargainPrice)

    }).filter(_._1 > 0.0)


    pfaceToBargain.toList

  }


}
