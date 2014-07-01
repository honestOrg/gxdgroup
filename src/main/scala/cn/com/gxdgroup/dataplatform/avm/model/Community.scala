package cn.com.gxdgroup.dataplatform.avm.model
import scala.beans.BeanProperty
import java.util.Date
import org.apache.commons.lang.StringUtils

/**
 * Created by ThinkPad on 14-5-20.
 */

//@SerialVersionUID(15L)
class Community(
                 val CommunityID: String,
                 val CommunityName: String,
                 val CountryName: String ,
                 val  Address: String ,
                 val LocationLongitude: Double ,
                 val LocationLatitude: Double,
                 val FeatureSoilRankValue:BigDecimal,
                 val FeatureBusCountValue:BigDecimal,
                 val FeatureParkValue:BigDecimal,
                 val FeatureAmenitiesValue:BigDecimal,
                 val FeatureTrafficControlValue:BigDecimal,
                 val FeatureGoodFactorValue:BigDecimal,
                 val FeatureBadFactorValue:BigDecimal,
                 val FeatureScopeValue:BigDecimal,
                 val FeatureBuildYearValue:BigDecimal,
                 val FeatureHeatingValue:BigDecimal,
                 val FeatureIsSchoolValue:BigDecimal,
                 val FeatureStyleValue:BigDecimal,
                 val FeaturePropertyLevelValue:BigDecimal,
                 val FeatureEnvironmentValue:BigDecimal,
                 val FeatureDensityValue:BigDecimal,
                 val FeatureFARValue:BigDecimal,
                 val FeatureDistanceFromCenterValue:BigDecimal,
                 val FeatureDistanceFromTradingValue:BigDecimal,
                 val FeatureDistanceFromLandScapeValue:BigDecimal,
                 var SimilarCommunity:List[AVMCommunity]
                 )extends Serializable{

  @BeanProperty var BargainList:List[Bargain]=null
 // def this() = this(CommunityID, CommunityName,Address)
}


case class AVMCommunity(){
  var Community:Community=null

  var Distance:Double = 1
}

case class Threshold(
                      val  id:Int ,
                      val  name:String,
                      val  CommunityStyle:Int,
                      val  SoilRank :Double,
                      val  BusCount :Double,
                      val  Park :Double,
                      val  Amenities :Double,
                      val  TrafficControl :Double,
                      val  GoodFactor :Double,
                      val  BadFactor :Double,
                      val  Scope :Double,
                      val  BuildYear :Double,
                      val  Heating :Double,
                      val  IsSchool :Double,
                      val  Style :Double,
                      val  PropertyLevel :Double,
                      val  Environment :Double,
                      val  Density :Double,
                      val  Far :Double,
                      val  DistanceFromCenter :Double,
                      val  DistanceFromTrading :Double,
                      val  DistanceFromLandScape :Double
                      )

case class Bargain (
                    val   id:String,
                    val   communityID:String,
                    val   square:Double,
                    //  val   FaceTo:String,
                    val   currentFloor:Int,
                    val   totalFloor:Int,
                    //  val   LocationLongitude:Double,
                    //  val   LocationLatitude:Double,
                    val   BuildYear:String,
                    //  注意是时间类型的
                    val  bargainTime:Date,
                    val   bargainPrice:BigDecimal
                    //  val   FaceToValue:BigDecimal
                    ){

  override def toString() = id+"\t"+communityID+"\t"+square+"\t"+"\t"+currentFloor+"\t"+totalFloor+"\t"+"\t"+"\t"+BuildYear+"\t"+bargainTime+"\t"+bargainPrice


}

case class Bargain2 (
                     val   id:String,
                     val   cityName:String,
                     val   CountyName:String,
                     val   communityID:String,
                     val communityName:String,
                     val   square:Double,
                     val   FaceTo:String,
                     val   currentFloor:Int,
                     val   totalFloor:Int,
                     val   LocationLongitude:Double,
                     val   LocationLatitude:Double,
                     val   BuildYear:String,
                     //  注意是时间类型的
                     val  bargainTime:Date,
                     val   bargainPrice:BigDecimal,
                     val   relationID:String
                     ){

  override def toString() = id+"\t"+communityID+"\t"+square+"\t"+"\t"+currentFloor+"\t"+totalFloor+"\t"+"\t"+"\t"+BuildYear+"\t"+bargainTime+"\t"+bargainPrice


}




case class AVMBargain(){
  var Weight: Double = 1
  var   Case:Bargain = null
  var   adjustPrice:BigDecimal=null

}

case class AVMBargain2(){
  var Weight: Double = 1
  var   Case:Bargain2 = null
  var   adjustPrice:BigDecimal=null

}


case class Section(
                    val  small:Double,
                    val  big:Double
                    )

case class Setting(
                    val  id:Int,
                    val  name:String,
                    val  maxMonth:Int,
                    val  maxDistance:Int,
                    val  bargainTimePower:Double,
                    val  diffrentBuilding:Double,
                    val  diffrentCommunity:Double,
                    val  distancePower:Double,

                    val buildYearCoefficient:Map[Double, Double],
                    val  floorCoefficient:Map[Int, Double],
                    val  floorRule:Map[Int, Section],
                    val  squareCoefficient:Map[Int, Double],
                    val squareRule:Map[Int, Section],
                    val squareAdjustCoefficient:Map[Double, Double]
                    )

case class Index(
                  val  id:Int,//编号
                  val  city:String,//城市
                  val  price:BigDecimal,//价格
                  val  dateTime:Date  //时间
                  )

case class Index2(

                  val  price:BigDecimal,//价格
                  val  dateTime:Date  //时间
                  )


case class Result(){
  var price:BigDecimal =0
  var list:List[AVMBargain] =null

}

case class Result2(){
  var price:BigDecimal =0
  var list:List[AVMBargain2] =null

}