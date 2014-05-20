package cn.com.gxdgroup.dataplatform.practice

/**
 * Created by wq on 5/20/14.
 */
object WqClass extends App{

  class Persion(val firstName: String, val lastName: String) {

    private var _age = 0
    def age = _age
    def age_=(newAge: Int) = _age = newAge

    def fullName() = firstName + " " + lastName

    override def toString() = fullName()
  }

  val obama: Persion = new Persion("Barack", "Obama")

  println("Persion: " + obama)
  println("firstName: " + obama.firstName)
  println("lastName: " + obama.lastName)
  obama.age_=(51)
  println("age: " + obama.age)
  //=====================================================================
  println("---------------------------------------------------------")

  def withClose(closeAble: { def close(): Unit;def getConnect(): Unit }, op: { def close(): Unit;def getConnect(): Unit } => Unit) {
    try {
      closeAble.getConnect()
      op(closeAble)
    } finally {
      closeAble.close()
    }
  }

  def withClose1(closeAble: {def close(): Unit}) (op: {def close(): Unit} => Unit){
    try {
      op(closeAble)
    } finally {
      closeAble.close()
    }
  }

  class Connection(con: String) { //{ def close(): Unit } 作为参数类型。因此任何含有close()的函数的类都可以作为参数。
  val aa = con
    def getConnect() = println("get Connectd")
    def close() = println("close Connection")
  }

  val conn: Connection = new Connection("DB connect")

  withClose(conn, conn => println("do something with Connection"))
  println("--------------------------------------")

  def withClose2[A <: { def close(): String;def get(): String }, B](closeAble: A) (f: A => B): B = {
    try {
      closeAble.get()
      f(closeAble)
    } finally {
      println(closeAble.close())
    }
  }

  class Connection1(con: String) {
    val aa = con
    def close(): String = "close Connection"
    def get() = "get"
    def xx() = println()
  }

  val conn1: Connection1 = new Connection1("DB connect"){

  }

  val msg = withClose2(conn1) (conn1 => {
    println("do something with Connection");
    "The End"
  })

  println(msg)
}