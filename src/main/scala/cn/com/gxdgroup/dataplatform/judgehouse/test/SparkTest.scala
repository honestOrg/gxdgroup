package cn.com.gxdgroup.dataplatform.judgehouse.test

import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import java.util.Date
import java.text.SimpleDateFormat

/**
 * Created by SZZ on 14-5-22
 */
object SparkTest {

  def main(args: Array[String]) = {
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
    //val sc = new SparkContext("spark://cloud40:7077", "test",
    //  System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val data = sc.textFile(input).map(x => DataProcress.processData(x))
    val error = data.filter {
      case (None, _) => true
      case _ => false
    }

    val succ = data.filter {
      case (None, _) => false
      case _ => true
    }

    val date = new Date()
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val timeStr = format.format(date)

    succ.map(p => p._1).saveAsTextFile(output + "/HOUSE/" + timeStr)
    succ.map(p => p._2).saveAsTextFile(output + "/BUILDING/" + timeStr)
    error.map(p => p._2).saveAsTextFile(output + "/ERROR/" + timeStr)
  }
}