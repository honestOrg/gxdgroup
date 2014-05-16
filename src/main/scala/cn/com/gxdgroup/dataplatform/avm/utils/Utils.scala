package cn.com.gxdgroup.dataplatform.avm.utils
import scala.math
/**
 * Created by ThinkPad on 14-5-16.
 */
object Utils {
  //向列表中添加元素
  def addItem(obj:Any):List[Any]={
    obj::Nil
  }
 // 计算距离
  def rad(d:Double):Double ={
   d * Math.PI / 180.0;
 }

 val  Earth_radius:Double =6378.137

  def GetDistance(lat1:Double,lng1:Double,lat2:Double,lng2:Double):Double ={
    val radLat1:Double = rad(lat1)
    val radLat2:Double = rad(lat2)
    val a = radLat1 - radLat2
    val b = rad(lng1) - rad(lng2)
    val s:Double = 2 * math.asin((math.sqrt(math.pow(math.sin(a / 2), 2) +math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2))))
    val s1 = s * Earth_radius
    math.round(s1 * 10000) / 10000
  }


}