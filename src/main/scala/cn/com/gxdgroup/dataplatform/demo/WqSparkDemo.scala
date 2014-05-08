package cn.com.gxdgroup.dataplatform.demo

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.{Text, LongWritable, IntWritable}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel


/**
 * Created by wq on 14-3-30.
 */
object WqSparkDemo {

  def main(args: Array[String]){

    if(args.length != 2){
      println("Usage : Input path output path")
      System.exit(0)
    }

    val spark = new SparkContext("local","my hadoop file",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    val file = spark.newAPIHadoopFile[LongWritable, Text, TextInputFormat](args(0))
    file.saveAsNewAPIHadoopFile[FileOutputFormat[LongWritable,Text]](args(1))
    file.saveAsHadoopFile[MultipleTextOutputFormatByKey[LongWritable,Text]](args(1))
    //MulipleTextOutputFormat
    file.saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[KeyBasedOutput[String, String]])



    val jars = ListBuffer[String]()
    jars+= args(2)

    val conf = new SparkConf()
    conf.setMaster("spark://honest:7077")
      .setSparkHome("/Users/wq/env/spark-0.9.0-incubating-bin-cdh4")
      .setAppName("wordcount")
      .set("spark.executor.memory","2g")
      //.setJars(jars)

    val sc = new SparkContext(conf)
    val counter = sc.accumulator(0)
    val data = sc.textFile(args(0))
    for(i <- 1 to 10){
      counter+=i
    }
    counter.value

    val broadcastVar = sc.broadcast(Array(1,2,3))
    broadcastVar.value

   //val date = new java.util.Date().getTime()

    data.persist(StorageLevel.DISK_ONLY)
    data.cache()
    println(data.count())
    data.filter(_.split(' ').length==3).map(_.split(' ')(1)).map((_,1)).reduceByKey(_+_)
    .map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).map(_._2).saveAsTextFile(args(2))

    //others

    data.filter(_.split(' ').length == 3).map(line => {val field = line.split(' ');(field(0),field(1))}).reduceByKey(_ + _)

  }

}
