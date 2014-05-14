package cn.com.gxdgroup.dataplatform.practice

/**
 * Created by wq on 14-5-13.
 */
object CastDemo {

  def main(args: Array[String]) {
    println(matchTest(1))

    wqcase()

    //caseStr()

    caseList
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
    println("---------------------------")
    List(("Mark",4),("Charles",5),("honest",4)).filter{
            case(name,number) => number == 4
    }.filter{case(x,t) => x=="honest"}.map(println _)
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

  case class Person(name: String, age: Int)

  class Person1(name:String,age:Int)

  def wqcase(){

    val honest = new Person1("Honest",34)
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

        case Person("Bob",32) => println("Hi Bob")
        case Person(name,age) => println(s"Age: $age year, name: $name !")

      }
    }

  }

}
