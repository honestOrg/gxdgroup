// package cn.com.gxdgroup.dataplatform.avm.function
//
//import org.apache.spark.SparkContext
//import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils._
//import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
//import org.apache.spark.rdd.RDD
//import cn.com.gxdgroup.dataplatform.avm.model._
//import java.util.Date
//
// /**
// * Created by ThinkPad on 14-5-27.
// */
//object GxdVerson2Driver {
//  def main(args: Array[String]){
////    if(args.length != 4){
////      println("Usage : Input path output path")
////      System.exit(0)
////    }
//
//
////    val conf = new SparkConf()
////    conf.setMaster("spark://cloud40:7077")
////      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
////      .setAppName("gxd")
////      .set("spark.executor.memory","2g").set("spark.default.parallelism","1")
////    val sc = new SparkContext(conf)
//    val sc = new SparkContext("spark://cloud40:7077", "gxd",
//      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
//    //初始化
//    val (indexes:RDD[(String,Seq[Index])],
//    thresholds:RDD[(String,Threshold)],
//    setting:Map[String,Setting],
//    communities:RDD[(Community,Array[AVMCommunity])],
//    bargains:RDD[(String,Seq[Bargain])])
//    = initAll(sc,args)
//
////    println("\n\n")
//   println("index:"+indexes.first)
//    println("\n\n")
//    println("thresholds:"+thresholds.first())
//    println("\n\n")
//    println("Setting:"+setting)
//    println("\n\n")
//    println("communities:" +communities.first._1.CommunityID+
//      ":ADDRESS"+communities.first._1.Address+
//     "CommunityName:"+communities.first._1.CommunityName
//    // +":BargainList"+communities.first._1.getBargainList.size
//     +":AVMCommunity Name:"+communities.first._2(0).Community.CommunityName+
//     ":AVMCommunity Distance"+communities.first._2(0).Distance)
//    println("communities:Count+++++++++++++:"+communities.count())
//   println("\n\n")
//    println("bargains:" + bargains.first._2(0).bargainTime.getTime)
//    println("bargainPrice:" + bargains.first._2(0).bargainPrice)
//
//    println("\n\n")
///**
//   process("怡美家园",14,18,88.00,"朝向","2004")
//
//def process(pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
//            pLocation_Longitude:Double=0,pLocation_Latitude:Double=0)={
//  println("\n\n************************************************\nstart\n************************************")
//  val  targetCommunity = communities.filter(line =>(line._1.CommunityName.equals(pcommunityName))).collect().take(1)
//  var firstResultList :List[((Double,Bargain,BigDecimal))] =Nil
//  var secondToBargainList :List[((Double,Bargain,BigDecimal))] =Nil
//
//  val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).first()
//
//println("\n\n************************************************\nFirst\n************************************")
//  if(!targetCommunity.isEmpty){
//    //其实只有一个
//    val  bargainList = bargains.filter(line =>
//      line._1 == (targetCommunity.head._1.CommunityID)).collect()(0)._2
// //   println("\n\n************************************************\n111111111111\n************************************")
//    firstResultList = calculateModel(bargainList,thresholds,setting,
//      pcommunityName,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
//  }
//  println("\n\n************************************************\nSecond\n************************************"
//          + firstResultList.size)
//
//  if(firstResultList.size < 5){
//
//    var similar:Double= setting("测试系数").diffrentCommunity
//    println("\n\n************************************************\nFive FIVE\n************************************")
//    val similarCommunityListReal = targetCommunity.isEmpty match {
//
//      case true => AVMUtils.GetSimilarCommunity(sc,communities,setting,pLocation_Longitude,pLocation_Latitude).collect()
//      case false =>  targetCommunity.head._2.filter(scl =>
//        Math.abs((targetCommunity.head._1.FeatureSoilRankValue - scl.Community.FeatureSoilRankValue).toDouble) <= BigDecimal(threadTarget._2.SoilRank)&&
//          Math.abs((targetCommunity.head._1.FeatureBusCountValue - scl.Community.FeatureBusCountValue).toDouble) <= BigDecimal(threadTarget._2.BusCount)&&
//          Math.abs((targetCommunity.head._1.FeatureParkValue - scl.Community.FeatureParkValue).toDouble) <= BigDecimal(threadTarget._2.Park)&&
//          Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
//          Math.abs((targetCommunity.head._1.FeatureTrafficControlValue - scl.Community.FeatureTrafficControlValue).toDouble) <= BigDecimal(threadTarget._2.TrafficControl)&&
//          Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
//          Math.abs((targetCommunity.head._1.FeatureGoodFactorValue - scl.Community.FeatureGoodFactorValue).toDouble) <= BigDecimal(threadTarget._2.GoodFactor)&&
//          Math.abs((targetCommunity.head._1.FeatureBadFactorValue - scl.Community.FeatureBadFactorValue).toDouble) <= BigDecimal(threadTarget._2.BadFactor)&&
//          Math.abs((targetCommunity.head._1.FeatureScopeValue - scl.Community.FeatureScopeValue).toDouble) <= BigDecimal(threadTarget._2.Scope)&&
//          Math.abs((targetCommunity.head._1.FeatureBuildYearValue - scl.Community.FeatureBuildYearValue).toDouble) <= BigDecimal(threadTarget._2.BuildYear)&&
//          Math.abs((targetCommunity.head._1.FeatureHeatingValue - scl.Community.FeatureHeatingValue).toDouble) <= BigDecimal(threadTarget._2.Heating)&&
//          Math.abs((targetCommunity.head._1.FeatureIsSchoolValue - scl.Community.FeatureIsSchoolValue).toDouble) <= BigDecimal(threadTarget._2.IsSchool)&&
//          Math.abs((targetCommunity.head._1.FeatureStyleValue - scl.Community.FeatureStyleValue).toDouble) <= BigDecimal(threadTarget._2.Style)&&
//          Math.abs((targetCommunity.head._1.FeaturePropertyLevelValue - scl.Community.FeaturePropertyLevelValue).toDouble) <= BigDecimal(threadTarget._2.PropertyLevel)&&
//          Math.abs((targetCommunity.head._1.FeatureEnvironmentValue - scl.Community.FeatureEnvironmentValue).toDouble) <= BigDecimal(threadTarget._2.Environment)&&
//          Math.abs((targetCommunity.head._1.FeatureDensityValue - scl.Community.FeatureDensityValue).toDouble) <= BigDecimal(threadTarget._2.Density)&&
//          Math.abs((targetCommunity.head._1.FeatureFARValue - scl.Community.FeatureFARValue).toDouble) <= BigDecimal(threadTarget._2.Far)&&
//          Math.abs((targetCommunity.head._1.FeatureDistanceFromCenterValue - scl.Community.FeatureDistanceFromCenterValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromCenter)&&
//          Math.abs((targetCommunity.head._1.FeatureDistanceFromTradingValue - scl.Community.FeatureDistanceFromTradingValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromTrading)&&
//          Math.abs((targetCommunity.head._1.FeatureDistanceFromLandScapeValue - scl.Community.FeatureDistanceFromLandScapeValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromLandScape))
//    }
//    similarCommunityListReal.isEmpty match{
//      case true => secondToBargainList =Nil
//      case false =>
//    //如果同一个小区，权值为1，否则为传入参数,可动态调整
//    similarCommunityListReal.map(scl => {
//      similar = similar * Math.pow((setting("测试系数").maxDistance + 1 - scl.Distance) / setting("测试系数").maxDistance, setting("测试系数").distancePower)
//
//      val  bargainList2 =  AVMUtils.GetBargain(bargains,scl.Community.CommunityID)
//      if(!bargainList2.isEmpty){
//      secondToBargainList = calculateModel(bargainList2,thresholds,setting,
//        pcommunityName,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
//      }else{
//        secondToBargainList =Nil
//      }
//      })
//    }
//  }
//  println("\n\n************************************************\nThird\n************************************")
//
//  //两个最终结果进行合并
//  val pfaceToBargainfinalList = firstResultList++secondToBargainList
//  println("\n\n************************************************\nFour+\n************************************")
//  println("\\n\\n***********************************\\npfaceToBargainfinalListPrice+\\n****************:"
//    +pfaceToBargainfinalList.head._3)
//  //留一个口,价格
//   var resultprice =Result()
//    resultprice.price=0
//    resultprice.list=null
//  val date_Cha = new Date().getTime-3600*24*90*1000l
//
//
//  val beijingIndexList = indexes.filter(aiml=> aiml._1.equals("北京")).take(1).head._2.toList
// println("beijingIndexList:"+beijingIndexList.head.price)
//  println("beijingIndexListGETtime:"+beijingIndexList.last.dateTime)
//  println("date_Cha:"+date_Cha)
//  //按时间进行降序排序
//  val newvalueList = beijingIndexList.filter{bjsi =>
//
//    println("liuliu::::::"+bjsi.dateTime.getTime)
//    bjsi.dateTime.getTime >= date_Cha
//  }.sortBy(x =>
//    - x.dateTime.getTime).take(1).toList
//
//  println("newvalueList@@@@@@@@@@@@:"+newvalueList.size)
//  val newValue =  newvalueList.isEmpty match
//  {
//    case true => 0
//    case false => newvalueList.head.price.toDouble
//  }
//
//  var fianlPriceAnd5house = null
//  if(!pfaceToBargainfinalList.isEmpty) {
//    // case true => List(0,new Bargain("0","0",0.0,0,0,"0",new Date(),BigDecimal(0)),BigDecimal(0))
//    //  case false =>pfaceToBargainfinalList.map(line =>{
//    val pfaceToBargainfinalList23 =  pfaceToBargainfinalList.map(line =>{
//      val oldvalueList = beijingIndexList.filter(index =>
//        index.dateTime.getYear == line._2.bargainTime.getYear&&
//          index.dateTime.getMonth ==line._2.bargainTime.getMonth
//      ).take(1).toList
//      val oldvalue = oldvalueList.isEmpty match{
//        case true => 0
//        case false => oldvalueList.head.price.toDouble
//      }
//
//      (line._1,line._2,line._2.bargainPrice * newValue / oldvalue)
//
//    })
//    println("23Size:" + pfaceToBargainfinalList23.size)
//    val pfaceToBargainfinalList22 = pfaceToBargainfinalList23.filter(ptb =>
//      ptb._2.bargainTime.getTime+6*30*3600 < new Date().getTime ).sortBy(x => - x._1).take(5).toList
//    println("*****:" + pfaceToBargainfinalList22(0)._3)
//    //这就是最终值
//    if(pfaceToBargainfinalList22.size >4){
//      var avmBagainFianlReault = AVMBargain()
//      var sumWeigth=0.0
//      var sumadjust_Price=0.0
//      var list5FianlBargain:List[AVMBargain] =List()
//      pfaceToBargainfinalList22.map(x =>{
//
//        sumWeigth +=x._1
//
//        avmBagainFianlReault.adjustPrice =x._3
//        println("adjustPrice: PRICE:"+x._3)
//        avmBagainFianlReault.Case = x._2
//        avmBagainFianlReault.Weight = x._1
//        list5FianlBargain = List(avmBagainFianlReault) ++ list5FianlBargain
//        // rst.Price = Math.Ceiling(cases.Sum(m => m.AdjustPrice * (decimal)m.Weight) / (decimal)sumWeight);
//      })
//      println("list5FianlBargain: SIZE:" +list5FianlBargain.size)
//      pfaceToBargainfinalList22.map(x =>{
//        sumadjust_Price +=(x._3.toDouble* x._1 /sumWeigth)
//
//      })
//      println("sumadjust_Price: SUM size:" +sumadjust_Price)
//      var sumadjust_Price11 = sumadjust_Price
//      //下面是最终的价钱
//      val priceFinal333 =  Math.ceil(sumadjust_Price11)
//
//
//      resultprice.price = priceFinal333
//      resultprice.list =list5FianlBargain
//      resultprice
//      println("price:"+resultprice.price)
//      println("Object resultprice:"+resultprice.list.head.Case.id)
//    }
//
//  }else{
//    resultprice
//    println("price222:"+resultprice.price)
//    println("Object resultprice222:"+resultprice)
//  }
//
//}
//**/
//}
//
//}
