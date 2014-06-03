package cn.com.gxdgroup.dataplatform.judgehouse.test

import java.security.MessageDigest
import scala.collection.mutable.ArrayBuffer

/**
 * Created by SZZ on 14-5-23
 */
object DataProcress {
  lazy private val MD5Handle = MessageDigest.getInstance("MD5")

  val hexChars = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')


  //求一个字符串的MD5码
  def MD5(in: String) = {
    val b = MD5Handle.digest(in.getBytes)
    val r = new ArrayBuffer[Char]()
    for (x <- b) {
      r += hexChars((x >>> 4) & 0xf)
      r += hexChars(x & 0xf)
    }
    r.mkString("")
  }

  //得到一个唯一的 MD5序列
  def randMd5Str() = {
    MD5(System.currentTimeMillis().toString)
  }

  //
  val numberPattern = "[0-9]+".r

  def findALLNumber(str: String) = {
    numberPattern.findAllIn(str).toArray
  }

  def processData(in: String) = {
    val t = in.split("\t", -1)
    val s = new Array[String](58)
    val k = new Array[String](18)
    if (t.length != 48 || in.startsWith("RECMETAID")) (None, in)
    else {
//      s(0) = randMd5Str()
      s(4) = t(4)
      s(13) = unapplyMeters(t(9))
      s(14) = unapplyMeters(t(10))
      val tem = findALLNumber(t(8))
      s(15) = if (tem.length > 0) tem(0) else ""
      s(16) = if (tem.length > 1) tem(1) else ""
      s(17) = if (tem.length > 3) tem(3) else ""
      s(50) = t(13).collect {
        case '东' => 'E'
        case '西' => 'W'
        case '南' => 'S'
        case '北' => 'N'
        case '环' => 'O'
        case '拐' => 'T'
      }
      s(24) = s(50) match {
        case "SN" => "1"
        case "NS" => "1"
        case "EW" => "1"
        case "WE" => "1"
        case _ => "0"
      }
      s(25) = if (s(15) != "" && (1 to 5).contains(s(15).toInt)) s(15) else "6"
      s(26) = s(17).trim match {
        case "平层" => "1"
        case "错层" => "2"
        case "复式楼" => "3"
        case "跃层" => "4"
        case _ => "5"
      }

      s(27) = unapplyMoney(t(6).trim)
      s(28) = unapplyMoney(t(7).trim)
      s(29) = formatTimeStr(t(5),timeFromat)
      s(31) = t(18).trim match {
        case "毛胚房" => "1"
        case "粗装修" => "2"
        case "中等装修" => "5"
        case "精装修" => "3"
        case "豪华装修" => "4"
        case _ => ""
      }
      s(39) = t(37)
      //还需要继续处理
      s(43) = t(25)
      s(44) = unapplyNumbers(t(16))
      s(45) = unapplyNumbers(t(15))
      s(46) = checkSource(t(2))
      s(52) = unapplyNumbers(t(12))
      //todo
      s(54) = t(21)
      s(55) = t(44)
      s(57) = t(11)

      //楼栋ID
      s(7) = ""
      k(0) = ""
      k(1) = t(23)
      k(2) = t(31)
      k(3) = s(45)
      (s.map(x => if (x == null) "" else x).mkString("|"), k.map(x => if (x == null) "" else x).mkString("|"))
      //      s.map(x => if (x == null) "" else x).mkString("|")
    }
  }

  //根据链接判断 数据来源
  def checkSource(in : String) = in match {
    case x if x.contains("soufun.com") => "搜房"
    case _ => ""
  }
  val timeFromat = "YYYY-MM-DD HH:MM:SS"
//    YYYY-MM-DD HH:MM:SSull
  def formatTimeStr(in : String, f : String) : String = {
    def format(in : String) = {
      if ((in.length % 2) != 0) "0" * (2 - in.length % 2)  + in
      else in
    }
    val x = in.split("\\D+").map(a => format(a))
    val fieldPatterns = f.split("\\w+")
    fieldPatterns.zip(x).map(p => p._1 + p._2).mkString
}
  /**
   * 从输入的数据中  提取出数字
   * @param in 输入的字符串
   * @return
   */
  def unapplyMeters(in: String) = in.collect {
    case ch if ch.isDigit => ch
    case '.' => '.'
  }

  def unapplyMoney(in : String) = {
   val (num, item) =  in.partition(ch => if (ch.isDigit || ch == '.') true else false)
   val  a = if (item.contains("万") || item.contains("W") || item.contains("w")) 10000 else 1
   a * (if (num == "") 0 else num.toDouble) + ""
  }

  //提取出数字
  def unapplyNumbers(in : String) = in.collect {
    case ch if ch.isDigit => ch
  }


  //  def parse(fileName: String, out: String) = {
  //
  //    val source = Source.fromFile(fileName)
  //    val pw = new PrintWriter(out)
  //    try {
  //      //      source.getLines().drop(1).map(t => processData(t)).filterNot(_ == "").foreach(p => pw.println(p))
  //      val data = source.getLines().drop(1).map(t => processData(t)).filter {
  //        case (null, null) => false
  //        case _ => true
  //      }
  //
  //    }
  //    finally source.close()
  //    pw.close()
  //  }


  //  def main(args: Array[String]) = {
  //    parse("d:/todo/ss.txt", "d:/todo/zz.txt")
  //  }
}
