//package cn.com.gxdgroup.dataplatform.avm.function
//
//import cn.com.gxdgroup.dataplatform.avm.model._
//import org.apache.spark.deploy.SparkHadoopUtil
//import org.apache.spark.SparkContext
//import org.apache.spark.scheduler.InputFormatInfo
//import org.apache.hadoop.mapred.TextInputFormat
//import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
//import java.util.{Calendar, Date}
//import org.apache.spark.SparkContext._
//import java.text.SimpleDateFormat
//import org.apache.spark.rdd.RDD
//import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate
//import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate2
//
//import cn.com.gxdgroup.dataplatform.avm.model.Bargain
//import cn.com.gxdgroup.dataplatform.avm.model.Threshold
//import cn.com.gxdgroup.dataplatform.avm.model.Setting
//import cn.com.gxdgroup.dataplatform.avm.model.AVMCommunity
//import cn.com.gxdgroup.dataplatform.avm.model.Index
//import org.apache.spark.storage.StorageLevel
//import org.apache.commons.lang.StringUtils
//
///**
// * Created by ThinkPad on 14-5-30.
// */on
//object TestSplit3Driver extends Serializable{
//  def main(args: Array[String]){
////        val conf = SparkHadoopUtil.get.newConfiguration()
////        val sc = new SparkContext("spark://cloud40:7077", "gxd", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass), Map(),
////          InputFormatInfo.computePreferredLocations(Seq(new InputFormatInfo(conf, classOf[TextInputFormat], args(0)))))
//    val sc = new SparkContext("spark://cloud40:7077", "gxd",
//           System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
////val sc = new SparkContext("spark://cloud40:7077", "gxd",
////
////  System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass),
////
////  Map("spark.serializer"->"org.apache.spark.serializer.KryoSerializer","spark.kryo.registrator"->"cn.com.gxdgroup.dataplatform.avm.model.AVMServialize"))
//
//    val GetAllIndex = sc.textFile(args(0)).cache()
//    val GetAllThreshold = sc.textFile(args(1)).cache()
//    val GetAllCommunity = sc.textFile(args(2)).cache()
//    val GetAllCommunity16 = sc.textFile(args(2),args(4).toInt).cache()
//    val GetAllBargainTextFile = sc.textFile(args(3)).cache()
//
//    val indexes= GetAllIndex.map(line => {
//      //先假设是逗号分隔
//      val IndexDetail_Arrays = line.split(",")
//      val index = Index(IndexDetail_Arrays(0).toInt,
//        IndexDetail_Arrays(1),
//        BigDecimal(IndexDetail_Arrays(2)),
//        AVMUtils.StringToDate(IndexDetail_Arrays(3))
//      )
//      val cityName = IndexDetail_Arrays(1)
//      (cityName,index)
//    }).groupByKey()
//   // val indexes =  sc.broadcast(AllIndexMapList).value
//    //println("indexes:"+indexes.count())
//    //println("AllIndexMapList:"+AllIndexMapList.count())
//
//
//
//
//    val thresholds= GetAllThreshold.map(line => {
//      //先假设是逗号分隔
//      val tt = line.split(",")
//      //此处还需要修改,其中tt(1)代表的是阈值的名
//      (tt(1),  Threshold(tt(0).toInt,tt(1),tt(2).toInt,
//        tt(3).toDouble,
//        tt(4).toDouble,
//        tt(5).toDouble,
//        tt(6).toDouble,
//        tt(7).toDouble,
//        tt(8).toDouble,
//        tt(9).toDouble,
//        tt(10).toDouble,
//        tt(11).toDouble,
//        tt(12).toDouble,
//        tt(13).toDouble,
//        tt(14).toDouble,
//        tt(15).toDouble,
//        tt(16).toDouble,
//        tt(17).toDouble,
//        tt(18).toDouble,
//        tt(19).toDouble,
//        tt(20).toDouble,
//        tt(21).toDouble))
//
//    })
//    //val thresholds = sc.broadcast(thresholdArray).value
//
//
//    //SETTING初始化
//    val buildYear_coefficient =Map[Double,Double]("0".toDouble -> 1,"1".toDouble -> 0.95,"2".toDouble ->0.9,"3".toDouble ->0.84,"4".toDouble ->0.8,"5".toDouble ->0.75,"6".toDouble->0.7,"7".toDouble->0.65,"8".toDouble->0.6,"9".toDouble-> 0.55,"10".toDouble-> 0.5,"11".toDouble-> 0.45,"12".toDouble-> 0.4,"13".toDouble-> 0.35,"14".toDouble-> 0.3,"15".toDouble-> 0.25,"16".toDouble-> 0.2,"17".toDouble-> 0.15,"18".toDouble-> 0.1)
//    val floor_coefficient = Map[Int,Double](0 ->1,1 ->0.85,2 ->0.6)
//    val floor_rule=Map[Int,Section](0 ->Section(0,10),1 ->Section(10,20),2 -> Section(20,100))
//    val square_coefficient=Map[Int,Double](0->1,1 ->0.85,2->0.6,3 ->0.3,4->0)
//    val square_rule=Map[Int, Section](0->Section ( 0, 40),1-> Section ( 40, 60 ),2-> Section (60, 90),3-> Section (90,140),4-> Section (140,999))
//    val square_adjust_coefficient=Map[Double,Double]("0".toDouble ->"1".toDouble,0.05-> 0.9,0.1-> 0.81,0.15-> 0.72,0.2-> 0.64,0.25-> 0.56,0.3-> 0.49,0.4-> 0.36,0.45-> 0.3,0.5-> 0.25,0.6-> 0.16,0.65-> 0.12,0.7-> 0.09,0.75-> 0.06,0.8-> 0.04,0.85-> 0.02,0.9-> 0.01,0.95-> "0".toDouble,"1".toDouble-> "0".toDouble,"100000".toDouble-> "0".toDouble)
//    val m_Setting = Map("测试系数" ->Setting(0,"测试系数",12,1,0.8,0.75,0.75,0.9,buildYear_coefficient,floor_coefficient,floor_rule,square_coefficient,square_rule,square_adjust_coefficient))
//
//    val setting = sc.broadcast(m_Setting).value
//
//   // println("GetAllCommunity111:"+GetAllCommunity.count())
//
//    //  Community初始化
//
//
//
//    //  GetAllCommunity***********
//    val AllCommunityTo_Array = GetAllCommunity.toArray()
//    val communities2=GetAllCommunity16.map(line =>{
//      //先假设是逗号分隔
//      val target = line.split("\t")
//    //  println("target(7):"+target.mkString("|"))
//      var community= new Community(target(0),
//        target(1),
//        target(2),
//        target(3),
//        target(4).toDouble,
//        target(5).toDouble,
//        BigDecimal(target(6)),
//        BigDecimal(target(7)),
//        BigDecimal(target(8)),
//        BigDecimal(target(9)),
//        BigDecimal(target(10)),
//        BigDecimal(target(11)),
//        BigDecimal(target(12)),
//        BigDecimal(target(13)),
//        BigDecimal(target(14)),
//        BigDecimal(target(15)),
//        BigDecimal(target(16)),
//        BigDecimal(target(17)),
//        BigDecimal(target(18)),
//        BigDecimal(target(19)),
//        BigDecimal(target(20)),
//        BigDecimal(target(21)),
//        BigDecimal(target(22)),
//        BigDecimal(target(23)),
//        BigDecimal(target(24)),null)
//      //其中4,5代表的是经纬度的数据数组下标到时候还需要改,0下标代表的是小区名
//
//      val SimilarCommunity =
//        AllCommunityTo_Array.map(desc =>(desc,
//
//          AVMUtils.GetDistance(target(4).toDouble,
//            target(5).toDouble,
//            desc.split("\t")(4).toDouble,
//            desc.split("\t")(5).toDouble)))
//          .filter(c =>(!c._1.split("\t")(0).
//          ==(target(0)))&&c._2<m_Setting("测试系数").maxDistance)
//      //下面avmModel
//      val avmModel = SimilarCommunity.map(line => {
//        val arrays = line._1.split("\t")
//        val community = new Community(arrays(0),
//          arrays(1),
//          arrays(2),
//          arrays(3),
//          arrays(4).toDouble,
//          arrays(5).toDouble,
//          BigDecimal(arrays(6)),
//          BigDecimal(arrays(7)),
//          BigDecimal(arrays(8)),
//          BigDecimal(arrays(9)),
//          BigDecimal(arrays(10)),
//          BigDecimal(arrays(11)),
//          BigDecimal(arrays(12)),
//          BigDecimal(arrays(13)),
//          BigDecimal(arrays(14)),
//          BigDecimal(arrays(15)),
//          BigDecimal(arrays(16)),
//          BigDecimal(arrays(17)),
//          BigDecimal(arrays(18)),
//          BigDecimal(arrays(19)),
//          BigDecimal(arrays(20)),
//          BigDecimal(arrays(21)),
//          BigDecimal(arrays(22)),
//          BigDecimal(arrays(23)),
//          BigDecimal(arrays(24)),null)
//        val distance:Double = line._2
//        val avmCommunity = AVMCommunity()
//        avmCommunity.Community = community
//        avmCommunity.Distance = distance
//        avmCommunity
//      })
//      //构造成具体的小区和它的相识的小区的键值对形式，其中line可以改成具体的小区名如target(1)
//      (community,avmModel)
//    })
////    val communities2 =communities1
////    val communities = communities2.cache()
//    //AllCommunity.persist(StorageLevel.MEMORY_ONLY_SER)
//    //val AllCommunityBroadcast = sc.broadcast(AllCommunity)
//
//   // val communities = sc.broadcast(AllCommunity).value
//
// //  println("communities:"+communities2.count())
//    //Bargain初始化
//    //取得所有Bargain，将其存在Map中其中key是小区名，value是list由所有的房子组成,注意要定义成var的
//    val bargains = GetAllBargainTextFile.map(line => {
//      val BargainDetail_Arrays = line.split("\t")
//      //这个之后还需要改，不确定具体的字段对应
//      val bargain = Bargain(
//        BargainDetail_Arrays(0),
//        BargainDetail_Arrays(1),
//        BargainDetail_Arrays(2).toDouble,
//        BargainDetail_Arrays(4).toInt,
//        BargainDetail_Arrays(5).toInt,
//        BargainDetail_Arrays(8),
//        AVMUtils.StringToDate2(BargainDetail_Arrays(9)),
//        BargainDetail_Arrays(10).toDouble)
//      val communityID = BargainDetail_Arrays(1)
//      (communityID,bargain)
//    }).groupByKey()
//   // val bargains = sc.broadcast(AllBargainMapList).value
////val bargains2 =bargains1
////    val bargains = bargains2.cache()
//  //  println("bargains:"+bargains.count())
//    process(communities2,"怡美家园",12,18,84.99,"朝向","2001")
// //  process(communities2,"怡美家园",14,18,88.00,"朝向","2004")
// //  process(communities2,"怡美家园",14,18,88.00,"朝向","2004")
//   // communities.values
//
//    def process(communities:RDD[(Community,Array[AVMCommunity])],pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
//                pLocation_Longitude:Double=0,pLocation_Latitude:Double=0)={
//      println("\n\n************************************************\nstart\n************************************")
//      val  targetCommunity = communities.filter(line =>(line._1.CommunityName.equals(pcommunityName))).take(1)
//      var firstResultList :List[((Double,Bargain,BigDecimal))] =Nil
//      var secondToBargainList :List[((Double,Bargain,BigDecimal))] =Nil
//
//      val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).first()
//
//      println("targetCommunitytargetCommunitytargetCommunitytarget" + targetCommunity(0)._1.CommunityID)
//
//      val communityId = targetCommunity(0)._1.CommunityID
//
//      println("\n\n************************************************\nFirst\n************************************")
//      if(!targetCommunity.isEmpty){
//        //其实只有一个
//        val  bargainList = bargains.filter(line =>
//          line._1 == communityId).collect()(0)._2
//        //   println("\n\n************************************************\n111111111111\n************************************")
//        firstResultList = calculateModel(bargainList,thresholds,setting,
//          pcommunityName,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
//      }
//      println("\n\n************************************************\nSecond\n************************************"
//        + firstResultList.size)
//
//      if(firstResultList.size < 5){
//
//        var similar:Double= setting("测试系数").diffrentCommunity
//        println("\n\n************************************************\nFive FIVE\n************************************")
//        val similarCommunityListReal = targetCommunity.isEmpty match {
//
//          case true => AVMUtils.GetSimilarCommunity(sc,communities,setting,pLocation_Longitude,pLocation_Latitude).collect()
//          case false =>  targetCommunity.head._2.filter(scl =>
//            Math.abs((targetCommunity.head._1.FeatureSoilRankValue - scl.Community.FeatureSoilRankValue).toDouble) <= BigDecimal(threadTarget._2.SoilRank)&&
//              Math.abs((targetCommunity.head._1.FeatureBusCountValue - scl.Community.FeatureBusCountValue).toDouble) <= BigDecimal(threadTarget._2.BusCount)&&
//              Math.abs((targetCommunity.head._1.FeatureParkValue - scl.Community.FeatureParkValue).toDouble) <= BigDecimal(threadTarget._2.Park)&&
//              Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
//              Math.abs((targetCommunity.head._1.FeatureTrafficControlValue - scl.Community.FeatureTrafficControlValue).toDouble) <= BigDecimal(threadTarget._2.TrafficControl)&&
//              Math.abs((targetCommunity.head._1.FeatureAmenitiesValue - scl.Community.FeatureAmenitiesValue).toDouble) <= BigDecimal(threadTarget._2.Amenities)&&
//              Math.abs((targetCommunity.head._1.FeatureGoodFactorValue - scl.Community.FeatureGoodFactorValue).toDouble) <= BigDecimal(threadTarget._2.GoodFactor)&&
//              Math.abs((targetCommunity.head._1.FeatureBadFactorValue - scl.Community.FeatureBadFactorValue).toDouble) <= BigDecimal(threadTarget._2.BadFactor)&&
//              Math.abs((targetCommunity.head._1.FeatureScopeValue - scl.Community.FeatureScopeValue).toDouble) <= BigDecimal(threadTarget._2.Scope)&&
//              Math.abs((targetCommunity.head._1.FeatureBuildYearValue - scl.Community.FeatureBuildYearValue).toDouble) <= BigDecimal(threadTarget._2.BuildYear)&&
//              Math.abs((targetCommunity.head._1.FeatureHeatingValue - scl.Community.FeatureHeatingValue).toDouble) <= BigDecimal(threadTarget._2.Heating)&&
//              Math.abs((targetCommunity.head._1.FeatureIsSchoolValue - scl.Community.FeatureIsSchoolValue).toDouble) <= BigDecimal(threadTarget._2.IsSchool)&&
//              Math.abs((targetCommunity.head._1.FeatureStyleValue - scl.Community.FeatureStyleValue).toDouble) <= BigDecimal(threadTarget._2.Style)&&
//              Math.abs((targetCommunity.head._1.FeaturePropertyLevelValue - scl.Community.FeaturePropertyLevelValue).toDouble) <= BigDecimal(threadTarget._2.PropertyLevel)&&
//              Math.abs((targetCommunity.head._1.FeatureEnvironmentValue - scl.Community.FeatureEnvironmentValue).toDouble) <= BigDecimal(threadTarget._2.Environment)&&
//              Math.abs((targetCommunity.head._1.FeatureDensityValue - scl.Community.FeatureDensityValue).toDouble) <= BigDecimal(threadTarget._2.Density)&&
//              Math.abs((targetCommunity.head._1.FeatureFARValue - scl.Community.FeatureFARValue).toDouble) <= BigDecimal(threadTarget._2.Far)&&
//              Math.abs((targetCommunity.head._1.FeatureDistanceFromCenterValue - scl.Community.FeatureDistanceFromCenterValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromCenter)&&
//              Math.abs((targetCommunity.head._1.FeatureDistanceFromTradingValue - scl.Community.FeatureDistanceFromTradingValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromTrading)&&
//              Math.abs((targetCommunity.head._1.FeatureDistanceFromLandScapeValue - scl.Community.FeatureDistanceFromLandScapeValue).toDouble) <= BigDecimal(threadTarget._2.DistanceFromLandScape))
//        }
//        similarCommunityListReal.isEmpty match{
//          case true => secondToBargainList =Nil
//          case false =>
//            //如果同一个小区，权值为1，否则为传入参数,可动态调整
//            similarCommunityListReal.map(scl => {
//              similar = similar * Math.pow((setting("测试系数").maxDistance + 1 - scl.Distance) / setting("测试系数").maxDistance, setting("测试系数").distancePower)
//
//              val  bargainList2 =  AVMUtils.GetBargain(bargains,scl.Community.CommunityID)
//              if(!bargainList2.isEmpty){
//                secondToBargainList = calculateModel(bargainList2,thresholds,setting,
//                  pcommunityName,pfloor,pTotalFloor,psquare,pfaceTo,pbuildYear,pLocation_Longitude,pLocation_Latitude)
//              }else{
//                secondToBargainList =Nil
//              }
//            })
//        }
//      }
//      println("\n\n************************************************\nThird\n************************************")
//
//      //两个最终结果进行合并
//      val pfaceToBargainfinalList = firstResultList++secondToBargainList
//      println("\n\n************************************************\nFour+\n************************************")
//      println("\\n\\n***********************************\\npfaceToBargainfinalListPrice+\\n****************:"
//        +pfaceToBargainfinalList.head._3)
//      //留一个口,价格
//      var resultprice =Result()
//      resultprice.price=0
//      resultprice.list=null
//      val date_Cha = new Date().getTime-3600*24*90*1000l
//
//
//      val beijingIndexList = indexes.filter(aiml=> aiml._1.equals("北京")).take(1).head._2.toList
//      println("beijingIndexList:"+beijingIndexList.head.price)
//      println("beijingIndexListGETtime:"+beijingIndexList.last.dateTime)
//      println("date_Cha:"+date_Cha)
//      //按时间进行降序排序
//      val newvalueList = beijingIndexList.filter{bjsi =>
//
//        println("liuliu::::::"+bjsi.dateTime.getTime)
//        bjsi.dateTime.getTime >= date_Cha
//      }.sortBy(x =>
//        - x.dateTime.getTime).take(1).toList
//
//      println("newvalueList@@@@@@@@@@@@:"+newvalueList.size)
//      val newValue =  newvalueList.isEmpty match
//      {
//        case true => 0
//        case false => newvalueList.head.price.toDouble
//      }
//
//      var fianlPriceAnd5house = null
//      if(!pfaceToBargainfinalList.isEmpty) {
//        // case true => List(0,new Bargain("0","0",0.0,0,0,"0",new Date(),BigDecimal(0)),BigDecimal(0))
//        //  case false =>pfaceToBargainfinalList.map(line =>{
//        val pfaceToBargainfinalList23 =  pfaceToBargainfinalList.map(line =>{
//          val oldvalueList = beijingIndexList.filter(index =>
//            index.dateTime.getYear == line._2.bargainTime.getYear&&
//              index.dateTime.getMonth ==line._2.bargainTime.getMonth
//          ).take(1).toList
//          val oldvalue = oldvalueList.isEmpty match{
//            case true => 0
//            case false => oldvalueList.head.price.toDouble
//          }
//
//          (line._1,line._2,line._2.bargainPrice * newValue / oldvalue)
//
//        })
//        println("23Size:" + pfaceToBargainfinalList23.size)
//        val pfaceToBargainfinalList22 = pfaceToBargainfinalList23.filter(ptb =>
//          ptb._2.bargainTime.getTime+6*30*3600 < new Date().getTime ).sortBy(x => - x._1).take(5).toList
//        println("*****:" + pfaceToBargainfinalList22(0)._3)
//        //这就是最终值
//        if(pfaceToBargainfinalList22.size >4){
//          var avmBagainFianlReault = AVMBargain()
//          var sumWeigth=0.0
//          var sumadjust_Price=0.0
//          var list5FianlBargain:List[AVMBargain] =List()
//          pfaceToBargainfinalList22.map(x =>{
//
//            sumWeigth +=x._1
//
//            avmBagainFianlReault.adjustPrice =x._3
//            println("adjustPrice: PRICE:"+x._3)
//            avmBagainFianlReault.Case = x._2
//            avmBagainFianlReault.Weight = x._1
//            list5FianlBargain = List(avmBagainFianlReault) ++ list5FianlBargain
//            // rst.Price = Math.Ceiling(cases.Sum(m => m.AdjustPrice * (decimal)m.Weight) / (decimal)sumWeight);
//          })
//          println("list5FianlBargain: SIZE:" +list5FianlBargain.size)
//          pfaceToBargainfinalList22.map(x =>{
//            sumadjust_Price +=(x._3.toDouble* x._1 /sumWeigth)
//
//          })
//          println("sumadjust_Price: SUM size:" +sumadjust_Price)
//          var sumadjust_Price11 = sumadjust_Price
//          //下面是最终的价钱
//          val priceFinal333 =  Math.ceil(sumadjust_Price11)
//
//
//          resultprice.price = priceFinal333
//          resultprice.list =list5FianlBargain
//          resultprice
//          println("price:"+resultprice.price)
//          println("Object resultprice:"+resultprice.list.head.Case.id)
//        }
//
//      }else{
//        resultprice
//        println("price222:"+resultprice.price)
//        println("Object resultprice222:"+resultprice)
//      }
//
//    }
//  }
//
//  def calculateModel(bargainList:Seq[Bargain],thresholds:RDD[(String,Threshold)],settingBroadcast:Map[String,Setting],pcommunityName:String, pfloor:Int, pTotalFloor:Int,psquare:Double,pfaceTo:String,pbuildYear:String,
//                     pLocation_Longitude:Double=0,pLocation_Latitude:Double=0):List[(Double,Bargain,BigDecimal)]={
//
//    val threadTarget = thresholds.filter(lines => lines._1.equals("测试阈值")).first()
//
//    val mineCoummunityBargain = bargainList.filter(line => (new Date().
//      getTime-line.bargainTime.getTime)>= settingBroadcast("测试系数").maxMonth*3600).filter(
//        (line => (line.totalFloor <= threadTarget._2.CommunityStyle
//          ||pTotalFloor>threadTarget._2.CommunityStyle)
//          &&(pTotalFloor<=threadTarget._2.CommunityStyle||
//          line.totalFloor>threadTarget._2.CommunityStyle))
//      )
//    println("\n\n************************************************\nfloorRule\n************************************")
//    val flRuleList = settingBroadcast("测试系数").floorRule.toList
//    println("\n\n************************************************\nfloorRuleAFTER\n************************************")
//    val flRulePoint_KeyVal = flRuleList.filter(flr =>
//      pfloor >= flr._2.small
//        &&pfloor<flr._2.big)
//
//    val flRulePoint_Key = flRulePoint_KeyVal.map(line => line match{
//      case (x:Int,y :Section) => x
//      case _ => 0
//    }).head
//
//    var weight:Double =1
//    //下面的这个是个Map
//    val flCoefficientList = settingBroadcast("测试系数").floorCoefficient
//    //楼层分段过滤开始
//    val bargainFloorRule = mineCoummunityBargain.map(cb =>{
//      weight=1
//      val flr_KeyVal=  flRuleList.filter(frl=>
//        cb.totalFloor>=frl._2.small&&
//          cb.totalFloor<frl._2.big)
//      val targetfloorIntKey = flr_KeyVal.isEmpty match {
//        case true => 0
//        case false => flr_KeyVal.head._1
//      }
//
//      weight = weight*flCoefficientList.get(Math.abs(flRulePoint_Key - targetfloorIntKey)).getOrElse(0.0)
//      (weight,cb)
//    }).filter(_._1 != 0.0)
//
//    //面积分段过滤开始
//    val squareList = settingBroadcast("测试系数").squareRule.toList
//
//    val pointsquare_KeyVal= squareList.filter(sL =>
//      psquare >= sL._2.small&&
//        psquare<sL._2.big )
//    //界面输入的值计算
//    val pointSqureInt = pointsquare_KeyVal.map(line => line match{
//      case (x:Int,y: Section) => x
//      //这个值是默认值
//      case _ =>0
//    }).head
//
//    val bargainSquare =bargainFloorRule.map(bfr =>{
//
//      val slTarget_KeyVal = squareList.filter(sL =>
//        bfr._2.square>= sL._2.small &&
//          bfr._2.square < sL._2.big)
//
//      val slTarget_KeyInt =  slTarget_KeyVal.map(line =>
//        line match{
//          case(x:Int,y:Section) => x
//          case _ => 0
//        }).head
//      //因为bfr._1是double的所以默认是0.0
//      weight =  bfr._1*flCoefficientList.get(Math.abs(pointSqureInt - slTarget_KeyInt)).getOrElse(0.0)
//      (weight,bfr._2)
//    }).filter(_._1 != 0.0)
//
//    //楼栋
//
//    //建成年份开始过滤, 输入是经过面积过滤后的,bargainSquare也应该进行判断是否为空
//    var PointbuildYear =0
//    var tagertbuildYear = 0
//    val buildYearCofficent = settingBroadcast("测试系数").buildYearCoefficient
//
//    val buildYearBargain = bargainSquare.map(bgs => {
//
//      if(StringUtils.isNumeric(pbuildYear)&& StringUtils.isNumeric(bgs._2.BuildYear)){
//        PointbuildYear = pbuildYear.toInt
//        tagertbuildYear = bgs._2.BuildYear.toInt
//
//        val buildCha = Math.abs(PointbuildYear-tagertbuildYear)
//        val buildKey = buildYearCofficent.get(buildCha).getOrElse(0.0)
//        weight = bgs._1 * buildKey
//        (weight,bgs._2)
//      }else{
//        (0.0,bgs._2)
//      }
//    }).filter(_._1 != 0.0)
//
//
//
//
//    //根据挂牌时间开始过滤，这里的buildYearBargain也应该判断是否为空
//    val maxMonth = settingBroadcast("测试系数").maxMonth
//    val timePowerBargain =settingBroadcast("测试系数").bargainTimePower
//    val  c = Calendar.getInstance()
//    val  c2 = Calendar.getInstance()
//    val timeBargain = buildYearBargain.map(byg => {
//      //这块会有一些问题，主要看你传入的日期是什么格式的
//      c2.setTime(byg._2.bargainTime)
//      weight =  byg._1*Math.pow((maxMonth +1 -((c.get(Calendar.YEAR)-c2.get(Calendar.YEAR))*12 +
//        (c.get(Calendar.MONTH) -c2.get(Calendar.MONTH))))*1.0/maxMonth,
//        timePowerBargain)
//
//      (weight,byg._2)
//    }).filter(_._1 !=0.0 )
//
//
//    // 根据面积开始过滤
//    val square_adjust_coefficientList =settingBroadcast("测试系数").squareAdjustCoefficient.toList.sorted
//
//    val square_adjust_coefficientBargain=  timeBargain.map(tb =>{
//
//      val psquare_Cha = (Math.abs(psquare - tb._2.square)/psquare)
//      // weight = tb._1* square_adjust_coefficient.get(psquareRound).getOrElse(0.0)
//      val square_adjust_List = square_adjust_coefficientList.filter(line => line._1 > psquare_Cha)
//      if(!square_adjust_List.isEmpty){
//
//        val square_adjust_weight = square_adjust_List.map(line => line match{
//          case (x:Double,y:Double) => y
//          case _ => 0.0
//        }).head
//        weight = tb._1*square_adjust_weight
//      }else{
//        weight = tb._1 *0.0
//      }
//      (weight,tb._2)
//    }).filter(_._1 != 0.0)
//
//
//    //朝向过滤开始,现在其实没有做，留个口
//
//    val pfaceToBargain =  square_adjust_coefficientBargain.map(line => {
//      weight = line._1*1
//      //(权重，bargain，bargainPrice)
//      println("line._2.bargainPrice%%%%%%%%%%%%%%%  :"+line._2.bargainPrice)
//      (weight,line._2,line._2.bargainPrice)
//
//    }).filter(_._1 > 0.0)
//
//
//    pfaceToBargain.toList
//
//  }
//
//  //不同小区根据经纬度求相似小区
//  def  GetSimilarCommunity( sc:SparkContext,communities:RDD[(Community,Array[AVMCommunity])],
//                            setting:Map[String,Setting],
//                            pLongitude:Double,
//                            pLatitude:Double): RDD[AVMCommunity] =
//  {
//    println("\n\n************************************************\nGetSimilarCommunity\n************************************")
//    communities.map(line => {
//      //key value对，其中key是Community，value是具体的距离
//      line._1
//      (line._1,AVMUtils.GetDistance(pLatitude,pLongitude,
//        line._1.LocationLatitude,line._1.LocationLongitude))
//    }).filter(_._2 < setting("测试系数").maxDistance).map(line2 => {
//      val avmc = AVMCommunity()
//      avmc.Community = line2._1
//      avmc.Distance =line2._2
//      avmc
//    })
//  }
//
//
//
//  def   GetBargain(bargains:RDD[(String,Seq[Bargain])],pCommunityID:String)=
//  {
//    if(!bargains.filter(abml => abml._1.equals(pCommunityID)).collect().isEmpty){
//
//
//      bargains.filter(abml => abml._1.equals(pCommunityID)).collect().head._2.toList
//    }else{
//      val a :Seq[Bargain] = List()
//      a
//    }
//
//  }
//
//}
//
//
