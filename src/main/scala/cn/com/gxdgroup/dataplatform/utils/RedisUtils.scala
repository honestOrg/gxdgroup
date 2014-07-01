package cn.com.gxdgroup.dataplatform.utils

import com.redis.RedisClientPool


/**
 * Created by SZZ on 14-6-16
 */
object RedisUtils {
  val propFile = "/config/redis.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val host = prop.getOrElse("REDIS.HOST", "192.168.1.41")
  val port = prop.getOrElse("REDIS.PORT", "6379").toInt

  val pool = new RedisClientPool(host, port).pool

  def getClient = {
    pool.getFactory.makeObject()
  }

  def putMap2RedisTable(tableName: String, map: Map[String, String]) = {
    val client = getClient
    client.pipeline {
      f => f.hmset(tableName, map)
    }
    pool.returnObject(client)
  }

  def getResultMap(tableName: String): Map[String, String] = {
    val client = getClient
    val res = client.hgetall(tableName)
    pool.returnObject(client)
    res match {
      case Some(map) => map
      case None => Map()
    }
  }

  def delTable(name: String) {
    val client = getClient
    if (client.exists(name))
      client.del(name)
  }

  private[this] def dataGenerator(loop: Int) = {
    val data = for (i <- 0 to loop) yield (i.toString, i.hashCode().toString)
    data.toMap
  }

  def main(args: Array[String]) {
    val map = dataGenerator(100000)
    delTable("szztest")
    println("start")
    val start = System.nanoTime()
    map.grouped(8000).foreach {
      x => putMap2RedisTable("szztest", x)
    }
    val end = System.nanoTime()
    val cost = end - start
    println(s"${cost / (1000 * 1000 * 1000)}s ${cost % (1000 * 1000 * 1000) / (1000 * 1000)} ms")
  }
}
