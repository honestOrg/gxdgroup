package cn.com.gxdgroup.dataplatform.practice

/**
 * Created by wq on 14-5-14.
 */
class TestApply {

  def apply()={
    println("is ok!")
  }

  def test{
    println("test")
  }

  private[this] val kn = 1

  val kn1 = 2

}

object TestApply {

}

class TestApply1 {
  def test1 {
    println("test1")
  }
}

object  TestApply1 {
  var i = 0;

  def apply() = {
    println("jjjjjj")
    new TestApply1
  }

  def add(){
    i = i+1
  }
}

object WqApply extends App{

  val t = new TestApply
  t()//等于调用类TestApply的apply方法
  t.test
  println(t.kn1)


  val a = TestApply1()//等于TestApply1.apply()
  a.test1


  for(i <- 1 to 10){
    TestApply1.add()
  }
  println(TestApply1.i)

}
