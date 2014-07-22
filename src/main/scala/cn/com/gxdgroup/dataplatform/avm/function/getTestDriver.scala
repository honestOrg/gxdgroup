package cn.com.gxdgroup.dataplatform.avm.function
import cn.com.gxdgroup.dataplatform.avm.model._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapred.TextInputFormat
import java.util.{Calendar, Date}
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils.StringToDate2

import cn.com.gxdgroup.dataplatform.avm.model.Bargain
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import cn.com.gxdgroup.dataplatform.avm.model.Setting
import cn.com.gxdgroup.dataplatform.avm.model.AVMCommunity
import cn.com.gxdgroup.dataplatform.avm.model.Index
import cn.com.gxdgroup.dataplatform.avm.model.Result
import cn.com.gxdgroup.dataplatform.avm.model.Section
import cn.com.gxdgroup.dataplatform.avm.model.Threshold
import cn.com.gxdgroup.dataplatform.avm.model.AVMBargain
import org.apache.spark.storage.StorageLevel
import org.apache.commons.lang.StringUtils
import scala.collection.mutable

import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}

import scala.concurrent.{Await, Future}


import scala.BigDecimal
import cn.com.gxdgroup.dataplatform.avm.utils.AVMUtils
import scala.collection.mutable.ArrayBuffer
import redis.clients.jedis.Tuple

/**
 * Created by ThinkPad on 14-6-12.
 */
object getTestDriver {
  def main(args: Array[String]){


    val sc = new SparkContext("spark://cloud40:7077", "gxd",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val initializesimlarCommunitys = sc.textFile(args(0))

//    val collectMapinitializesSC =  initializesimlarCommunitys.map(line => {
//      var lines = line.split(",")
//      (lines(0),line)
//    }).groupByKey().collectAsMap()
        val collectMapinitializesSC =  initializesimlarCommunitys.map(line => {
          var lines = line.split(",")
          (lines(0),line)
        }).count

//    val lis = List("123")
//
//    sc.parallelize(lis).map(line =>{
//      collectMapinitializesSC.get(line)
//    }).count()



  }
}
