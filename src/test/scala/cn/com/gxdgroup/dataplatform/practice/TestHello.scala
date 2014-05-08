package cn.com.gxdgroup.dataplatform.practice

import org.scalatest.FunSuite

/**
 * Created by wq on 14-5-6.
 */
class TestHello extends FunSuite{

  test("sayHello"){
    val hello = new Hello
    assert(hello.sayHello("scala") == "hello,scala")
  }

}
