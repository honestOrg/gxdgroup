package cn.com.gxdgroup.dataplatform.etl.function

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

/**
 * Created by SZZ on 14-6-10
 */
object PinYinHelper {

  def getHeadPinyinChar(chiniseWord: String) = {
    var res = new ArrayBuffer[Char]()
    for (ch <- chiniseWord) {
      val pinyinArr = toHanyuPinyinStringArray(ch)
      pinyinArr match {
        case None => res += ch
        case Some(values) => res += values(0)(0)
      }
    }
    res.mkString
  }

  def toHanyuPinyinStringArray(c: Char) = {
    getUnformattedHanyuPinyinStringArray(c)
  }

  def getUnformattedHanyuPinyinStringArray(c: Char) = {
    ChineseToPinyinResource.getHanyuPinyinStringArray(c)
  }

  def main(args: Array[String]) {
    println(getHeadPinyinChar("宋琢琢"))
  }
}


import scala.collection.JavaConversions.propertiesAsScalaMap

object ChineseToPinyinResource {

  val resourceName = "/config/unicode_to_hanyu_pinyin.txt"
  val unicodeToHanyuPinyinTable: scala.collection.Map[String, String] = {
    val prop = new Properties()
    prop.load(this.getClass.getResourceAsStream(resourceName))
    prop
  }

  def getHanyuPinyinStringArray(c: Char) = {
    val pinyinRecord = getHanyuPinyinRecordFromChar(c)
    pinyinRecord match {
      case "" => None
      case ch =>
        val indexOfLeftBracket = pinyinRecord.indexOf("(")
        val indexOfRightBracket = pinyinRecord.lastIndexOf(")")
        val stripedString = pinyinRecord.substring(indexOfLeftBracket
          + "(".length, indexOfRightBracket)
        Some(stripedString.split(","))
    }
  }

  def getHanyuPinyinRecordFromChar(c: Char): String = {
    // convert Chinese character to code point (integer)
    val codePointofChar = c.toInt
    val codePointHexStr = Integer.toHexString(codePointofChar).toUpperCase
    // fetch from hashtable
    val foundRecord = unicodeToHanyuPinyinTable.getOrElse(codePointHexStr, "")
    foundRecord match {
      case "(none0)" => ""
      case ch if !ch.startsWith("(") || !ch.endsWith(")") => ""
      case ch => ch
    }
  }
}