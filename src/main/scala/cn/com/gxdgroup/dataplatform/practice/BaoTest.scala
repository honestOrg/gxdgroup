package cn.com.gxdgroup.dataplatform.practice

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.commons.lang.StringUtils
import org.objenesis.strategy.StdInstantiatorStrategy
import redis.clients.jedis.Jedis

import scala.collection.mutable.MutableList

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
  k.setRegistrationRequired(false)
  k.setInstantiatorStrategy(new StdInstantiatorStrategy())
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
  println("/****************************************/")


  k.register(classOf[MutableList[Person]])

  val p1 = new Person("zhangsan", 20)
  val p2 = new Person("zhangsan1", 21)
  val p3 = new Person("zhangsan2", 22)
  val p4 = new Person("zhangsan3", 23)
  val p5 = new Person("zhangsan4", 24)
  val p6 = new Person("zhangsan5", 25)
  val list = MutableList(p1, p2, p3, p4, p5)

  val out2 = new Output(1, 4096)
  k.writeObject(out2, list)
  jedis.set("ltest".getBytes(), out2.toBytes)

  val bytes2 = jedis.get("ltest".getBytes())
  val listResult = k.readObject(new Input(bytes2), classOf[MutableList[Person]])
  listResult.foreach{
    x =>
      println(x.name)
      println(x.age)
      println("----------------------")
  }


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