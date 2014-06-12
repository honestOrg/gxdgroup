package cn.com.gxdgroup.dataplatform.etl

import cn.com.gxdgroup.dataplatform.etl.function.EstateProcess
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat


/**
 * Created by SZZ on 14-6-6
 */
object EstateEtl {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("USAGE [master]  input output")
      System.exit(-1)
    }

    val Array(master, input, output): Array[String] = args.length match {
      case 2 => Array("spark://cloud40:7077", args(0), args(1))
      case 3 => Array(args(0), args(1), args(2))
    }

    val conf = SparkHadoopUtil.get.newConfiguration()
    val sc = new SparkContext(master, "judgeHouse", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass), Map(),
      InputFormatInfo.computePreferredLocations(Seq(new InputFormatInfo(conf, classOf[TextInputFormat], input))))
    sc.textFile(input, 1).map(line => EstateProcess.processData(line)).filter {
      case "" => false
      case _ => true
    }.saveAsTextFile(output)
  }
}
