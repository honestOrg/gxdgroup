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
import cn.com.gxdgroup.dataplatform.avm.utils.{JedisUtils, AVMUtils}
import scala.collection.mutable.ArrayBuffer
import redis.clients.jedis.{Jedis, Tuple}
/**
* Created by ThinkPad on 14-6-12.
*/
object FunctionByRedis {
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


//    val collectMapCommunity = GetAllCommunity.map(line =>{
//      var lines = line.split("\t")
//      (lines(0),line)
//    }).collectAsMap()
/*
    val collectMapinitializesSC =  initializesimlarCommunitys.map(line => {
      var lines = line.split(",")
      (lines(0),line)
    }).groupByKey().collectAsMap()
  */
    JedisUtils.initPool
    val j: Jedis = JedisUtils.getJedis
    val collectMapinitializesSC= j.hgetAll("AVMINIT")
    val collectMapCommunity= j.hgetAll("COMMUNITYINIT")
    val allBargainsByCommunityID= j.hgetAll("BARGAINSINIT")

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
    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.85,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
    val floor_coefficient = Map[Int,Double](-1 -> 0,0 ->1,1 ->0.85,2 ->0.6)
    val floor_rule=Map[Int,Section](-1 -> Section(-100,0),0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
    val square_coefficient=Map[Int,Double](-1 -> 0,0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999),-1-> Section (999,999999))
    val square_adjust_coefficient=Map[Double,Double](0.0 ->1.0,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
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


    var listBargainFirst:List[String]=Nil
    //怡美家园34FB10C8-28FB-4458-935A-4AC28C4BE8B7	CDE1A25B-1359-4E59-AC85-6A71C534859D	84.99	东南	12	18	116.3284912	40.04579544	2001	2013/12/28	42358
    //批量计算开始
    val lg = AlotOFBargainList.map{
      line =>
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
        val psquare =  bargainDetialArrays(2).toDouble
        val pfaceTo =bargainDetialArrays(3)
        val pbuildYear = bargainDetialArrays(8)

        val  targetCommunityList =collectMapCommunity.get(pcommunityID)

        //val  targetSimilarCommunityList:Seq[String] = collectMapinitializesSC.get(pcommunityID).getOrElse(null)

        var firstResultList :List[((Double,Bargain,BigDecimal))] =Nil
        var secondToBargainList :List[((Double,Bargain,BigDecimal))] =Nil

        val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).head
        //     println("\n\n************************************************\nFirst\n************************************")
        //  println("\n\n************************************************\ntargetCommunityList\n************************************:"+targetCommunityList.size)

        val  bargainList:Seq[Bargain] = bargains.get(pcommunityID).getOrElse(null)
        if(targetCommunityList != null && bargainList != null){

          firstResultList = mapCalculateModel(bargainList,thresholds,setting,
            pcommunityID,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
          //  println("firstResultList:::::"+firstResultList.size)
        }

        //    println("\n\n************************************************\nSecond\n************************************")
        //        + firstResultList.size)
        if(firstResultList.size < 5){
          var similar:Double= setting("测试系数").diffrentCommunity
          //     println("\n\n************************************************\nFive FIVE\n************************************")
          val similarCommunityListReal:List[(String,Double)] = targetCommunityList==null match {
            case true => GetSimilarCommunity2(GetAllCommunityToArray,setting,pLocation_Longitude,pLocation_Latitude).toList
            // case true => List()
            case false =>
              val array42Fields = collectMapCommunity.get(pcommunityID).split("\t")
              collectMapinitializesSC.get(pcommunityID).split(",").filter{x=>
                val distanceAndCommID= x.split("\t")
                val communityDetail = collectMapCommunity.get(distanceAndCommID(1)).split("\t")
                (
                    Math.abs((array42Fields(1).toDouble- communityDetail(1).toDouble)) <= BigDecimal(threadTarget._2.SoilRank)&&
                    Math.abs((array42Fields(2).toDouble - communityDetail(2).toDouble)) <= BigDecimal(threadTarget._2.BusCount)&&
                    Math.abs((array42Fields(3).toDouble - communityDetail(3).toDouble)) <= BigDecimal(threadTarget._2.Park)&&
                    Math.abs((array42Fields(4).toDouble - communityDetail(4).toDouble)) <= BigDecimal(threadTarget._2.Amenities)&&
                    Math.abs((array42Fields(5).toDouble - communityDetail(5).toDouble)) <= BigDecimal(threadTarget._2.TrafficControl)&&
                    Math.abs((array42Fields(6).toDouble - communityDetail(6).toDouble)) <= BigDecimal(threadTarget._2.GoodFactor)&&
                    Math.abs((array42Fields(7).toDouble - communityDetail(7).toDouble)) <= BigDecimal(threadTarget._2.BadFactor)&&
                    Math.abs((array42Fields(8).toDouble - communityDetail(8).toDouble)) <= BigDecimal(threadTarget._2.Scope)&&
                    Math.abs((array42Fields(9).toDouble - communityDetail(9).toDouble)) <= BigDecimal(threadTarget._2.BuildYear)&&
                    Math.abs((array42Fields(10).toDouble - communityDetail(10).toDouble)) <= BigDecimal(threadTarget._2.Heating)&&
                    Math.abs((array42Fields(11).toDouble - communityDetail(11).toDouble)) <= BigDecimal(threadTarget._2.IsSchool)&&
                    Math.abs((array42Fields(12).toDouble - communityDetail(12).toDouble)) <= BigDecimal(threadTarget._2.Style)&&
                    Math.abs((array42Fields(13).toDouble - communityDetail(13).toDouble)) <= BigDecimal(threadTarget._2.PropertyLevel)&&
                    Math.abs((array42Fields(14).toDouble - communityDetail(14).toDouble)) <= BigDecimal(threadTarget._2.Environment)&&
                    Math.abs((array42Fields(15).toDouble - communityDetail(15).toDouble)) <= BigDecimal(threadTarget._2.Density)&&
                    Math.abs((array42Fields(16).toDouble - communityDetail(16).toDouble)) <= BigDecimal(threadTarget._2.Far)&&
                    Math.abs((array42Fields(17).toDouble - communityDetail(17).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromCenter)&&
                    Math.abs((array42Fields(18).toDouble - communityDetail(18).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromTrading)&&
                    Math.abs((array42Fields(19).toDouble - communityDetail(19).toDouble)) <= BigDecimal(threadTarget._2.DistanceFromLandScape)
                  )
              }.map{line =>
              val distanceAndCommID= line.split("\t")
                (distanceAndCommID(1),distanceAndCommID(0).toDouble)
              }.toList
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
        //   println("\\n\\n***********************************\\npfaceToBargainfinalListPrice+\\n****************:"+pfaceToBargainfinalList.size)

        //留一个口,价格
        var resultprice =Result()
        resultprice.price=0
        resultprice.list=null
        val  subtrack3month = Calendar.getInstance()
        subtrack3month.setTime(new Date())
        subtrack3month.add(Calendar.MONTH,-3)
        // val date_Cha = new Date().getTime-3600*24*90*1000l


        val beijingIndexList = indexes.filter(aiml=> aiml._1.equals("北京")).take(1).head._2.toList
        //  println("beijingIndexList:"+beijingIndexList.head.price)
        //  println("beijingIndexListGETtime:"+beijingIndexList.last.dateTime)
        // println("date_Cha:"+date_Cha)
        //按时间进行降序排序
        val newvalueList = beijingIndexList.filter{
          bjsi =>

          //  println("liuliu::::::"+bjsi.dateTime.getTime)
            bjsi.dateTime.getTime >= subtrack3month.getTimeInMillis
        }.sortBy(x =>
          - x.dateTime.getTime).take(1).toList

        //  println("newvalueList@@@@@@@@@@@@:"+newvalueList.size)
        val newValue =  newvalueList.isEmpty match
        {
          case true => 0
          case false => newvalueList.head.price.toDouble
        }

        var fianlPriceAnd5house = null
        val  add6month = Calendar.getInstance()

        if(!pfaceToBargainfinalList.isEmpty) {
          // case true => List(0,new Bargain("0","0",0.0,0,0,"0",new Date(),BigDecimal(0)),BigDecimal(0))
          //  case false =>pfaceToBargainfinalList.map(line =>{
          val pfaceToBargainfinalList22 =  pfaceToBargainfinalList.map(line =>{
            add6month.setTime(line._2.bargainTime)
            add6month.add(Calendar.MONTH,6)

            if(add6month.getTimeInMillis< new Date().getTime){
              //  println("world cup!!!!!!!!!!!!!!!!!!!!!!!!!")
              val oldvalueList = beijingIndexList.filter(index =>

                index.dateTime.getYear == line._2.bargainTime.getYear&&
                  index.dateTime.getMonth ==line._2.bargainTime.getMonth
              ).take(1).toList
              val oldvalue = oldvalueList.isEmpty match{
                case true => 0
                case false => oldvalueList.head.price.toDouble
              }
              (line._1,line._2,line._2.bargainPrice * newValue / oldvalue)
            }else{
              (line._1,line._2,line._2.bargainPrice)
            }

          }).sortBy(x => - x._1).take(5).toList

          //   println("22Size:" + pfaceToBargainfinalList22.size)
          //这就是最终值
          if(pfaceToBargainfinalList22.size >4){

            var sumWeigth=0.0
            var sumadjust_Price=0.0
            var list5FianlBargain:List[AVMBargain] =List()
            pfaceToBargainfinalList22.map(x =>{
              var avmBagainFianlReault = AVMBargain()
              sumWeigth +=x._1

              avmBagainFianlReault.adjustPrice =x._3
              //     println("adjustPrice: PRICE:"+x._3)
              //    println("privePrice: PRICE:"+x._2.bargainPrice)
              avmBagainFianlReault.Case = x._2
              avmBagainFianlReault.Weight = x._1
              //    println("&&&&&&&&&&&&&&&&:"+avmBagainFianlReault.Case.id)
              list5FianlBargain = List(avmBagainFianlReault) ++ list5FianlBargain

              // rst.Price = Math.Ceiling(cases.Sum(m => m.AdjustPrice * (decimal)m.Weight) / (decimal)sumWeight);
            })
            //     println("list5FianlBargain: SIZE0:" +list5FianlBargain(0).Case.id)
            //   println("list5FianlBargain: SIZE1:" +list5FianlBargain(1).Case.id)
            //   println("list5FianlBargain: SIZE2:" +list5FianlBargain(2).Case.id)
            //   println("list5FianlBargain: SIZE3:" +list5FianlBargain(3).Case.id)
            //   println("list5FianlBargain: SIZE4:" +list5FianlBargain(4).Case.id)
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
            //  println("price:"+resultprice.price)
            // println("Object resultprice:"+resultprice.list.head.Case.id)
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
    val lg2 = lg.map{

      x => x._1

        //      println("lg/gxd/tableOne:::"+x._1)
        //        println("lg/gxd/tableOnex._2:::"+x._2)
        //        println("lg/gxd/tableOnex._3:::"+x._3)
        //        println("lg/gxd/tableOnex._4:::"+x._4)
        //        println("lg/gxd/tableOnex._5:::"+x._5)
        //        println("lg/gxd/tableOnex._6:::"+x._6)

        x._1
    }
    lg2.saveAsTextFile("lg/gxd/first")




    val lg3 = lg.filter(x => x._6 != "").flatMap{
      // val lg3 = lg.flatMap{
      x =>
        List(x._2,x._3,x._4,x._5,x._6)
    }

    lg3.saveAsTextFile("lg/gxd/second")





    //  println("listBargainFirst%%%%%%%%%%%%%%%%%%%%%:"+listBargainFirst.size)

    // sc.parallelize(listBargainFirst).saveAsTextFile("lg/gxd/liu")


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
    //   println("\n\n************************************************\nGetSimilarCommunity\n************************************")
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
    //  println("mineCoummunityBargain$$$$$$$$:"+mineCoummunityBargain.size)
    //  println("\n\n************************************************\nfloorRule\n************************************")
    val flRuleList = settingBroadcast("测试系数").floorRule.toList
    //println("\n\n************************************************\nfloorRuleAFTER\n************************************")
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
        cb.currentFloor>=frl._2.small&&
          cb.currentFloor<frl._2.big)
      val targetfloorIntKey = flr_KeyVal.isEmpty match {
        case true => 0
        case false => flr_KeyVal.head._1
      }

      weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
      //   println("楼层分段::::"+cb.id+":"+weight)
      (weight,cb)
    }).filter(_._1 != 0.0)

    //println("bargainFloorRule%%%%%%%%%%%:"+bargainFloorRule.size)
    //面积分段过滤开始
    val squareList = settingBroadcast("测试系数").squareRule.toList
    val squareCoffientMap = settingBroadcast("测试系数").squareCoefficient
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

      weight =  bfr._1*squareCoffientMap.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)

      //  println("面积分段::::"+bfr._2.id+":"+weight)
      (weight,bfr._2)
    }).filter(_._1 != 0.0)
    //println("bargainSquare**********:"+bargainSquare.size)
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
        // println("建成年份::::"+bgs._2.id+":"+weight)
        (weight,bgs._2)
      }else{
        (0.0,bgs._2)
      }
    }).filter(_._1 != 0.0)

    //println("buildYearBargain!!!!!!!!:"+buildYearBargain.size)


    //根据挂牌时间开始过滤，这里的buildYearBargain也应该判断是否为空
    val maxMonth = settingBroadcast("测试系数").maxMonth
    val timePowerBargain =settingBroadcast("测试系数").bargainTimePower
    val  c = Calendar.getInstance()
    val  c2 = Calendar.getInstance()
    val timeBargain = buildYearBargain.map(byg => {
      //这块会有一些问题，主要看你传入的日期是什么格式的
      c2.setTime(byg._2.bargainTime)
      //   println("c2.get(Calendar.YEAR):"+c2.get(Calendar.YEAR))
      //  println("c2.get(Calendar.MONTH):"+c2.get(Calendar.MONTH))
      weight =  byg._1*Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-c2.get(Calendar.YEAR))*12 +
        (c.get(Calendar.MONTH) -c2.get(Calendar.MONTH))))*1.0/maxMonth,
        0.8)
      //    println("挂牌时间::::"+byg._2.id+":"+weight)
      // println("timePowerBargain:"+timePowerBargain)
      (weight,byg._2)
    }).filter(_._1 !=0.0 )

    //println("timeBargain2222222222:"+timeBargain.size)
    // 根据面积开始过滤
    val square_adjust_coefficientList =settingBroadcast("测试系数").squareAdjustCoefficient.toList.sorted

    val square_adjust_coefficientBargain=  timeBargain.map(tb =>{
      //  println("tb._2.square#########:"+tb._2.square)
      // println("psquare:"+psquare)
      val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
      //println("psquare_Cha++++++++:"+psquare_Cha)
      // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)

      val square_adjust_List = square_adjust_coefficientList.filter{
        line =>
        //   println("line._1:"+line._1)
          line._1 >= psquare_Cha

      }
      if(!square_adjust_List.isEmpty){
        //println("777777777777777777")
        val square_adjust_weight = square_adjust_List.map(line => line match{
          case (x:Double,y:Double) =>
            //          println("doubl66666666666666666e")
            y
          case _ => 0.0
        }).head
        weight = tb._1*square_adjust_weight
      }else{
        weight = tb._1 *0.0
        //  println("为空。。。。。。。。。")
      }
      //  println("面积开始过滤::::"+tb._2.id+":"+weight)
      (weight,tb._2)
    }).filter(_._1 != 0.0)
    //println("square_adjust_coefficientBargain333333333333"+square_adjust_coefficientBargain.size)

    //朝向过滤开始,现在其实没有做，留个口

    val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
      weight = line._1*1
      //(权重，bargain，bargainPrice)
      //     println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice+"\t"+weight)
      (weight,line._2,line._2.bargainPrice)

    }).filter(_._1 > 0.0)

    //println("pfaceToBargain4444444444444:"+pfaceToBargain.size)
    pfaceToBargain.toList

  }

}
