package cn.com.gxdgroup.dataplatform.avm.function

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by ThinkPad on 14-6-5.
 */
object CollectAsMapTestDriver {
  def main(args: Array[String]){

     println("a"+","+"b"+mkdirdou(2))
    def mkdirdou(num:Int):String={
      var dou =""
      for(i <- 0 to num){
        dou = dou+","
      }
      dou
    }
  }
}
