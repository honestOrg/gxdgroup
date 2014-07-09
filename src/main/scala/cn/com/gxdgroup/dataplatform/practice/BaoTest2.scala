package cn.com.gxdgroup.dataplatform.practice

import java.util.HashMap

import redis.clients.jedis.Jedis

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.Map

/**
 * Created by bao on 14-6-26.
 */
object BaoTest2 {
  def main(args: Array[String]) {
    //    mapTest
    //    redisTest
    redisTest2
    //    redisTest3
  }

  def redisTest3 {
    val l = System.currentTimeMillis()
    val j = new Jedis("192.168.1.41", 6379)
    val l2 = System.currentTimeMillis()
    println("connect cost:" + (l2 - l))
    val a = j.hgetAll("上海市小区表")
    val b = j.hgetAll("上海市案例表")
    val c = j.hgetAll("上海市AVMINIT")
    val d = j.lrange("上海", 0, -1)
    val l3 = System.currentTimeMillis()
    println("get cost:" + (l3 - l2))
    println("total cost:" + (l3 - l))
  }

  def redisTest2 {
    import com.redis._

    val r = new RedisClient("192.168.1.41", 6379)

    r.hgetall("test")
    val map = r.hgetall("test") match {
      case Some(map) if !map.isEmpty => map
      case Some(map) if map.isEmpty => {
        r.hgetall("baotest") match {
          case Some(map) => map
          case None => Map.empty
        }
      }
    }

    println(map.mkString(","))

  }

  def redisTest {
    val j = new Jedis("192.168.1.41", 6379)

    var map = j.hgetAll("aaa")


    map.isEmpty match {
      case true => map = j.hgetAll("hash")
      case false => map
    }
    println(map.mkString(","))


  }

  def mapTest {
    val list = List("a", "b", "c")
    val map: Map[String, String] = new HashMap[String, String]

    for (key <- list) {
      map(key) = map.getOrElse(key, "fk") + "e"
    }

    println(map.mkString(","))
  }
}
