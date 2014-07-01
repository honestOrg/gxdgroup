package cn.com.gxdgroup.dataplatform.practice

import java.util.Date
import java.text.SimpleDateFormat

/**
 * Created by wq on 14-5-13.
 */
object CastDemo {

  def kankan() : String = {

    val myMap1 = Map("key1" -> "value1")

    val value11 = myMap1.get("key1")

    value11.getOrElse("cc")
  }

  def kankan1(): Person = {

    val myMap1 = Map("key1" -> Person("Alice",25))

    val value11 = myMap1.get("key1")

    value11.getOrElse(new Person())

  }

  def main(args: Array[String]) {
    println(matchTest(1))
    println("--------------")
    println("nomatch:"+matchTest1())
    println("--------------")
    wqcase()
    println("--------------")
    //caseStr()

    caseList

    println("!!!!!!!!!!!!!!")
    isEmptyList()

    caseNull()

    val a = List(1,2,3,4)
    a.reduce((x,y)=>x+y)
    a.reduce(_+_)
    println("left:"+(0 /: a){(x,y) => x+y})
    println("right:"+(a :\ 0){(x,y) => x+y})

    (0 /: a){(x,y) => println("kankan left:"+y);1}
    (a :\ 0){(x,y) => println("kankan right:"+x);1}

  }


  def caseNull() {
    val myMap = Map("key1" -> "value1")

    val value1 = myMap.get("key1")
    val value2 = myMap.get("key2")
    val value3 = myMap.get("key3")

    val result1 = value1.getOrElse(0)
    val result2 = value2.getOrElse(0)
    val result3 = value3 match {
      case Some(n) => n
      case None => 1
    }

    println(result1)
    println(result2)
    println(result3)
  }

  def isEmptyList(){   // case null
    val list = List((1,"1"),(2,"2"))
    val list2 = {
      val dd =list.filter{
        case (x:Int,y:String) => x >3
      }
      dd.isEmpty match {
        case true => 0
        case false => dd.head._1
      }
    }

    println("list2:"+list2)

    //println("cccc:"+list.take(1))
//    val kan = list2.isEmpty match {
//      case true => 0
//      case false => list2.head._1
//    }

    //println(kan)
//    val kankan = list.filter{
//      case(x:Int,y:String) => !list.isEmpty
//      //case _ => !list.isEmpty
//    }.take(1)
//    println(kankan)

//    val list1 = List()
//    val kankan1 = list1.map{
//      case(x:Int,y:String) => x
//      case _ => 0
//    }.take(1)
//    println(kankan1)
  }

  def caseList(){
    val alice = Person("Alice",25)
    val bob = Person("Bob",32)
    val charlie = Person("Charlie",32)
    val list = List(alice,bob,charlie)
    //list.map(x => {x match {case Person("Bob",12) => println("dddd")} })
    //list.map{_ match {case Person("Bob",12) => println("dddd")} }
    list.map{case Person(name,age) => println("dddd")}
    //list.filter{case Person("Alice",25)=> println("ccc")}
    List(("Mark",4),("Charles",5),("honest",4)).filter{
      case(name,number) => number == 4 && name =="honest"
    }.map(println _)
    println("---------------------------!")
    List(("Mark",4),("Charles",5),("honest",4)).filter{
            case(name,number) => number == 4
    }.filter{case(name,number) => name =="honest"}.map(println _)
  }

//  def caseStr(){
//    val str = "aa,31,bb,cc"
//    val a = str.split(",")
//    a match {
//      case x: Array => {
//        if(a.length==4){
//          println(x(0))
//        }
//      }
//    }
//  }

  def matchTest(x: Any): Any  = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
    case _ => "many"
  }

  def matchTest1(x: Any): Any = x match {
    case Some(1) => "one"
    case _ => "haha"
  }

  case class Person(name: String, age: Int){
    def this(){
      this("",11)
    }
  }

  class Person1(name:String,age:Int)

  def wqcase(){

    val honest = new Person1("Honest",34)
    val honest1 = new Person()
    val honest2 = Person(_,_)
    honest match {
      case x: Person1 => println("honest")
    }


    val alice = Person("Alice",25)
    val bob = Person("Bob",32)
    val charlie = Person("Charlie",32)

    List(alice,bob,charlie).filter{
      case Person(name,age) => {
        age>25
      }
    }.filter{
      case Person(name,age) => {
        name=="Bob"
      }
    }.map(println _)

    for(person <- List(alice,bob,charlie)){
      person match {
        //case Person(_,_) => println("easy type!!")
        //case Person("Alice",25) => println("Hi Alice")
        case Person(name,age) => {
          if(age >25){
            println("aaaa:"+age)
          }

          age > 25  match {
            case x => println("xqq:"+x)
            //case None => None
          }
        }

//        case Person("Bob",32) => println("Hi Bob")
//        case Person(name,age) => println(s"Age: $age year, name: $name !")

      }
    }

  }

}
