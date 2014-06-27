package cn.com.gxdgroup.dataplatform.practice

import redis.clients.jedis.{Pipeline, Jedis, JedisPool, JedisPoolConfig}

/**
 * Created by wq on 14-6-5.
 */
object JedisDemo {

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
    pool = new JedisPool(config, "cloud41", 6379)
  }

  def getJedis: Jedis = {
    pool.getResource()
  }

  def close(pool: JedisPool, r: Jedis) = {
    if (r != null)
      pool.returnResourceObject(r)
  }

  def withConnection[A](block: Jedis => Unit) = {
    implicit var redis = this.getJedis
    try {
      block(redis)
    } catch{
      case e : Exception => System.err.println(e)  //should use log in production
//      case _ => //never should happen
    }finally {
      this.close(pool, redis)
    }
  }

  def destroyPool = {
    pool.destroy
  }

}
