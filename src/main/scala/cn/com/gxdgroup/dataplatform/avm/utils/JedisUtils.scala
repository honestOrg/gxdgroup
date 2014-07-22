package cn.com.gxdgroup.dataplatform.avm.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig, Pipeline, Jedis}

/**
 * Created by ThinkPad on 14-6-12.
 */
object JedisUtils {
  def main(args: Array[String]) {

    val start: Long = System.currentTimeMillis
    initPool

    val j: Jedis = getJedis
    println("connect time:" + (System.currentTimeMillis - start))

    withConnection{j =>
      val start1: Long = System.currentTimeMillis
      val pipe: Pipeline = j.pipelined

      for(i <- 1 to  100000){
        pipe.hset("test33", Integer.toString(i), Integer.toString(i))
      }
      pipe.sync

      println("scala time:" + (System.currentTimeMillis - start1))
    }

    destroyPool

    /////////////////////////

  }

  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxActive(60)
  config.setMaxIdle(1000)
  config.setMaxWait(10000)
  config.setTestOnBorrow(true)

  var pool : JedisPool = null

  def initPool = {
   pool = new JedisPool(config, "cloud41", 6379,10*1000)
  }

  def getJedis: Jedis = {
    pool.getResource()
  }

  def close( r: Jedis) = {
    if (r != null)
      pool.returnResourceObject(r)
  }

  def withConnection[A](block: Jedis => Unit) = {
   implicit var redis = new JedisPool(config, "cloud41", 6379).getResource
    try {
      block(redis)
    } catch{
      case e : Exception => System.err.println(e)  //should use log in production
      case _ => //never should happen
    }finally {
      this.close( redis)
    }
  }

  def destroyPool = {
    pool.destroy
  }

}
