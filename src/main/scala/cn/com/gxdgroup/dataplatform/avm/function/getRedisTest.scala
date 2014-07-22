package cn.com.gxdgroup.dataplatform.avm.function

import redis.clients.jedis.Jedis
import cn.com.gxdgroup.dataplatform.avm.utils.JedisUtils

/**
 * Created by ThinkPad on 14-6-26.
 */
object getRedisTest {

  def main(args: Array[String]) {
    val time =System.currentTimeMillis()

    JedisUtils.initPool
//    val j: Jedis = JedisUtils.getJedis

    val j = new Jedis("192.168.1.41", 6379)
  //  val str:String = "金华市小区表,梅州市案例表,保山市AVMINIT,莱芜市小区表,丽江市AVMINIT,湖州市AVMINIT,烟台市小区表,塔城地区AVMINIT,锡林郭勒盟AVMINIT,黄冈市AVMINIT,四平市案例表,无锡市小区表,衡水市小区表,吐鲁番地区小区表,泉州市案例表,乐东黎族自治县案例表,肇庆市AVMINIT,惠州市AVMINIT,梅州市AVMINIT,潍坊市小区表,惠州市小区表,绍兴市小区表,临夏回族自治州小区表,鞍山市小区表,襄樊市AVMINIT"
//    val array:Array[String] = str.split(",")
//    array.map{line=>
//    j.del(line)
//    }
//    val b = j.hgetAll("上海市小区表")
//    val c = j.hgetAll("上海市案例表")
//    val d = j.lrange("上海",0,-1)

println("222222222:"+j.hgetAll("广州市案例表"))
println("length:"+j.hlen("广州市案例表"))
println(System.currentTimeMillis()-time)


  }

  }
