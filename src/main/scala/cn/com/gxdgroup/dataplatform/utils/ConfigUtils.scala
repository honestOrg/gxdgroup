package cn.com.gxdgroup.dataplatform.utils

import java.util.Properties
import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * Created by SZZ on 14-6-16
 */
object ConfigUtils {
  def getConfig(path: String): scala.collection.Map[String, String] = {
    val prop = new Properties()
    val inputStream = this.getClass.getResourceAsStream(path)
    try {
      prop.load(inputStream)
      prop
    } finally inputStream.close()
  }
}
