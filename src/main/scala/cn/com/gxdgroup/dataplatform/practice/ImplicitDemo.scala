package cn.com.gxdgroup.dataplatform.practice

/**
 * Created by wq on 5/17/14.
 */
class ImplicitDemo {

}

class InsertA(insert : ImplicitDemo){

  def add(msg : String){
    println("msg:"+msg)
  }

}

object test1 extends App{
  implicit def kkk(insert : ImplicitDemo) = new InsertA(insert)
  val test = new ImplicitDemo
  test.add("wq1")
  //------------------------------------------------------

  def testParam(str: String)(implicit name: String){
    println("kankan:"+str+"::"+name)
  }

  implicit val name1 = "wq"

  testParam("hehe")
  testParam("aaa")("bbbk")

  def test1(implicit str1: Int){
    println(str1)
  }
  implicit val str1 = 1111
  test1


  //-------------------------------------------------------
  implicit class Train(a: Int){
    def add(a: Int) : Int  = a+a
  }

  println(2.add(2))
}