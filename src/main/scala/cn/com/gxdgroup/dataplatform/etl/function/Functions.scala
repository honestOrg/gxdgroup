package cn.com.gxdgroup.dataplatform.etl.function

import scala.collection.mutable.ArrayBuffer
import java.security.MessageDigest

/**
 * Created by SZZ on 14-6-3
 */
object Functions {
  private[this] val MD5Handle = MessageDigest.getInstance("MD5")
  val hexChars = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  //求一个字符串的MD5码
  private[this] def MD5(in: String) = {
    val b = MD5Handle.digest(in.getBytes)
    val r = new ArrayBuffer[Char]()
    for (x <- b) {
      r += hexChars((x >>> 4) & 0xf)
      r += hexChars(x & 0xf)
    }
    r.mkString
  }

  //得到一个唯一的 MD5序列
  private[this] def currentTimeMD5(): String = MD5(System.currentTimeMillis().toString)

  // just value of in
  private[this] def value(in: String) = in

  //得到汉字拼音的大写首字母
  private[this] def getHeadPinYinChar(in: String) = {
    PinYinHelper.getHeadPinyinChar(in)
  }

  //提取数字
  private[this] def number(in: String) = in.collect {
    case ch if ch.isDigit || ch == '.' => ch
  }

  //  private[this] def format(in : Double, str : String) = str.format(in * 100) +  "%"

  private[this] val wuCanFucMap = Map[String, String]("currentTimeMD5" -> currentTimeMD5)

  private[this] val map = Map[String, (String) => String](
    "MD5" -> MD5,
    "number" -> number,
    "getHeadPinYinChar" -> getHeadPinYinChar,
    "parseBusNumbers" -> parseBusNumbers
  )

  def getWuCanFunc(name: String) = {
    wuCanFucMap.getOrElse(name, "")
  }

  def getFunc(name: String) = {
    map.getOrElse(name, defaultFunction)
  }

  val defaultFunction = value _


  val endStr1 = "地铁"
  val endStr2 = "轨道"
  val endStr3 = "等"

  private[this] def findMinPositiveIntger(arr: Array[Int]) = {
    var min = Integer.MAX_VALUE
    for (t <- arr) {
      if (t >= 0 && t < min) min = t
    }
    min match {
      case Integer.MAX_VALUE => None
      case x => Some(x)
    }
  }

  private[this] def parseBusNumbers(in: String) = {
    val endIndex1 = in.indexOf(endStr1)
    val endIndex2 = in.indexOf(endStr2)
    val endIndex3 = in.indexOf(endStr3)
    val str = findMinPositiveIntger(Array(endIndex1, endIndex2, endIndex3)) match {
      case None => in
      case Some(x) => in.substring(0, x)
    }

    val buff = new ArrayBuffer[String]()
    var flag = false
    var s = ""
    for (ch <- str) {
      if (ch.isLetterOrDigit || Character.getType(ch) == Character.OTHER_LETTER) {
        s += ch
        flag = true
      } else {
        if (flag) {
          flag = false
          buff += s
          s = ""
        }
      }
    }
    buff += s
    //    buff
    buff.size.toString
  }

  def main(args: Array[String]) {
    val s = " 394路, 664路、996路、运通114、运通101 992路、362路地铁"
    val b = parseBusNumbers(s)
    println(b)
  }

}
