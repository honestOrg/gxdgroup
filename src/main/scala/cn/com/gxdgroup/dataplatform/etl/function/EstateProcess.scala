package cn.com.gxdgroup.dataplatform.etl.function

import scala.io.Source

/**
 * Created by SZZ on 14-6-5
 */

object EstateProcess {
  val prop = Source.fromInputStream(this.getClass.getResourceAsStream("/config/estate.properites")).getLines().toArray

  def processData(in: String) = {
    val dest = new Array[String](79)
    val src = in.split("\t", -1)
    if (src.length != 50) ""
    else {
      for (args <- prop) {
        args match {
          case ArgMents(destIndex, srcIndex, func) =>
            destIndex match {
              case "" => Unit
              case _ =>
                srcIndex match {
                  case "" =>
                    func match {
                      case "" => Unit
                      case _ => val function = Functions.getWuCanFunc(func)
                        dest(destIndex.toInt) = function()
                    }
                  case _ =>
                    func match {
                      case "" => dest(destIndex.toInt) = src(srcIndex.toInt)
                      case _ => val function = Functions.getFunc(func)
                        dest(destIndex.toInt) = function(src(srcIndex.toInt))
                    }
                }
            }
          case ArgMents(destIndex, firstIndex, secondIndex, func) =>
            val first = if (firstIndex.contains("-")) dest(-firstIndex.toInt) else src(firstIndex.toInt)
            val second = if (secondIndex.contains("-")) dest(-secondIndex.toInt) else src(secondIndex.toInt)
            val function = Functions.getTwoArgsFunc(func)
            dest(destIndex.toInt) = function(first, second)

          case _ => Unit
        }
      }
      dest.map(x => if (x == null) "" else x).mkString("|")
    }
  }

  def todo() {
    val source = Source.fromFile("d:/todo/sss.txt")
    val in = source.getLines().drop(1)
    try {
      println("start")
      in.map(line => processData(line)).filterNot(_ == "").foreach(p => println(p))
    } finally {
      source.close()
    }
  }
}


