package cn.com.gxdgroup.dataplatform.practice

/**
 * Created by bao on 14-5-14.
 */
class BaoTest {
  private var name: String = null
  println(BaoTest.age)
}

object BaoTest extends App {
  private var age: Int = 0

  val b = new BaoTest()

  def test(): Int = {
    val list = List(1,2,3,4,5,6)
    list.find(_ > 2 ).getOrElse(2)
  }

}