package cn.com.gxdgroup.dataplatform.etl.function

import scala.collection.mutable.ArrayBuffer
import java.security.MessageDigest

/**
 * Created by SZZ on 14-6-3
 */
object Functions {
  //MessageDigest 非线程安全
  //  private[this] val MD5Handle = MessageDigest.getInstance("MD5")
  private[this] val hexChars = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  //求一个字符串的MD5码
  private[this] def MD5(in: String) = {
    val MD5Handle = MessageDigest.getInstance("MD5")
    val bytes = in.getBytes
    MD5Handle.update(bytes)
    val b = MD5Handle.digest()
    val r = new ArrayBuffer[Char]()

    for (x <- b) {
      r += hexChars((x >>> 4) & 0xf)
      r += hexChars(x & 0xf)
    }
    r.mkString
  }

  //得到一个唯一的 MD5序列
  private[this] def currentTimeMD5() = MD5(System.nanoTime().toString)

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

  private[this] val wuCanFucMap = Map[String, () => String]("currentTimeMD5" -> currentTimeMD5 _)

  private[this] val map = Map[String, (String) => String](
    "MD5" -> MD5,
    "number" -> number,
    "getHeadPinYinChar" -> getHeadPinYinChar,
    "parseBusNumbers" -> parseBusNumbers
  )


  private[this] val defaultFunction = value _

  private[this] val endStr1 = "地铁"
  private[this] val endStr2 = "轨道"
  private[this] val endStr3 = "等"

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

  private[this] def divWithFormat(first: String, second: String, format: String = "%.2f") = {
    try {
      val res = first.toDouble / second.toDouble
      format.format(res * 100) + "%"
    } catch {
      case _: Exception => ""
    }
  }

  private[this] def divWithDeafultFormat(first: String, second: String) = {
    divWithFormat(first, second)
  }

  private[this] val twoArgsMap = Map[String, (String, String) => String](
    "divWithDeafultFormat" -> divWithDeafultFormat
  )

  private[this] val threeArgMap = Map[String, (String, String, String) => String](
    "divWithFormat" -> divWithFormat
  )

  def getWuCanFunc(name: String) = wuCanFucMap.getOrElse(name, () => "")

  def getFunc(name: String) = {
    map.getOrElse(name, defaultFunction)
  }

  def getTwoArgsFunc(name: String) = {
    twoArgsMap.getOrElse(name, (first: String, second: String) => "")
  }

  def getThreeArgsFunc(name: String) = {
    threeArgMap.getOrElse(name, (first: String, second: String, third: String) => "")
  }

  def main(args: Array[String]) {
    //    val func = getFunc("getHeadPinYinChar")
    //    println(func("宋琢琢"))
    println(divWithDeafultFormat("1", "2"))
  }
}
