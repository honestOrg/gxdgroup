package cn.com.gxdgroup.dataplatform.practice

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis

/**
 * Created by bao on 14-5-14.
 */
class BaoTest {
//  private var name: String = null
//  println(BaoTest.age)
}

object BaoTest extends App {

  val k = new Kryo()
  k.register(classOf[Person])
  val person = new Person("ssssssss",40)
//  person.name = "zhaoliu"
//  person.age = 30

  val out = new Output(1, 4096)
  k.writeObject(out, person)

  val jedis = new Jedis("192.168.1.41", 6379)
  jedis.hset("kryotest".getBytes(), "zhangsan".getBytes(), out.toBytes)
  val bytes = jedis.hget("kryotest".getBytes(), "zhangsan".getBytes())

//  jedis.set("zhaoliu".getBytes, out.toBytes)
//  val bytes = jedis.get("zhaoliu".getBytes())
  val pp = k.readObject(new Input(bytes), classOf[Person])

  println(pp.name)
  println(pp.age)




//  private var age: Int = 0
//
//  val b = new BaoTest()
//
//  def test(): Int = {
//    val list = List(1,2,3,4,5,6)
//    list.find(_ > 2 ).getOrElse(2)
//  }

}

class Person (val name: String = StringUtils.EMPTY, val age: Int = Int.MinValue)
  extends Serializable {
//  def this() = this(StringUtils.EMPTY, Int.MinValue)
}

class Person3() {
  var name: String = StringUtils.EMPTY
  var age: Int = Int.MinValue
}