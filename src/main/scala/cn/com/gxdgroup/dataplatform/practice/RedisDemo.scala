package cn.com.gxdgroup.dataplatform.practice

import com.redis._

import serialization._

import Parse.Implicits._
import com.redis.cluster.{ClusterNode, NoOpKeyTag, KeyTag, RedisCluster}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Success
import scala.concurrent.duration._

/**
 * Created by wq on 5/16/14.
 */
object RedisDemo extends App {

  //testSimply

  //testpool

  var start = System.currentTimeMillis()

  looptest1

  println("scala time is:"+(System.currentTimeMillis()-start)+" ms")


  def testSimply {
    val r = new RedisClient("cloud41", 6379)

    r.set("key", "value")

    println(r.get("key").getOrElse("null"))

    r.hmset("map", Map("f" -> "1", "f1" -> "2"))

    println(r.hmget[String, String]("map", "f", "f1").getOrElse("null"))

    r.hmset("hash", Map("f" -> "1", "f1" -> 2))

    println(r.hmget[String, Int]("map", "f", "f1").getOrElse("null"))
  }

  def looptest1{

    var start = System.currentTimeMillis()

    val r = new RedisClient("cloud41", 6379)

    //val r1 = new RedisClientPool("cloud41", 6379)

    //val r =r1.pool.borrowObject()

    println("connect time :"+(System.currentTimeMillis()-start)+" ms")


    var start1 = System.currentTimeMillis()

    r.pipeline{ p =>
      for(i <-1 to 100){
        p.hset("test11",i.toString,i.toString)
      }
    }

    println("doing time :"+(System.currentTimeMillis()-start1)+" ms")

    //val timeout = 2 minutes

    /*val x = r.pipelineNoMulti(
      List(
        {() => for(i <-1 to 1000001){
          r.hset("test3",i.toString,i.toString)
          }
        }
      )
    )
    val result = x.map{a => Await.result(a.future, timeout)}*/


  }

  def looptest {

    val clients = new RedisClientPool("cloud41", 6379)

    for(i <-1 to 100){

      clients.withClient {
        client => {
          client.hset("test2",i.toString,i.toString)
        }
      }

    }


  }



  def testpool{

    var list = List("test1","test2")

    lp(list)

  }


  def lp(msgs: List[String]) = {
    val clients = new RedisClientPool("cloud41", 6379)
    clients.withClient {
      client => {
        msgs.foreach(client.lpush("list-l", _))
        println(client.llen("list-l").getOrElse("null"))

      }
    }
  }

}
