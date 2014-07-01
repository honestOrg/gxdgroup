package cn.com.gxdgroup.dataplatform.practice

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.twitter.chill.EnumerationSerializer
import org.objenesis.strategy.StdInstantiatorStrategy

/**
 * Created by wq on 14-6-13.
 */
object KryoDemo {

  class Student11(
                 var name: String,
                 var sex: String,
                 var age: Int
                 )extends Serializable{

  }

  class Student(
    var name: String,
    var sex: String,
    var age: Int
  )extends Serializable{
//    @BeanProperty var name1 = this.name
//    @BeanProperty var sex1 = this.sex
//    @BeanProperty var age1 = this.age
    def this() = this("","",0)
  }

  def main(args: Array[String]) {
    //test1()
    test2()
  }

  def test2(){
    val kryo = new Kryo()
    val output = new Output(1,4096)
    //val reg = kryo.register(classOf[Student])
    kryo.setRegistrationRequired(false)
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    val student = new Student11("zhang111","man",22)
    kryo.writeObject(output,student)
    val bb = output.toBytes
    output.flush()
    //----------------------------------------
    val input = new Input(bb)
    val stu = kryo.readObject(input,classOf[Student11])
    println(stu.name)
    //val kk = stu.asInstanceOf[Student]
    //println(kk.name+","+kk.sex+","+kk.age)
    //println(kk.getName+","+kk.getSex+","+kk.getAge)
  }

  def test1(){
    val kryo = new Kryo()
    val output = new Output(1,4096)
    val reg = kryo.register(classOf[Student])
    val student = new Student("zhang","man",22)
    kryo.writeObject(output,student)
    val bb = output.toBytes
    output.flush()
    //----------------------------------------
    val input = new Input(bb)
    val stu = kryo.readObject(input,classOf[Student])
    println(stu.name)
    //val kk = stu.asInstanceOf[Student]
    //println(kk.name+","+kk.sex+","+kk.age)
    //println(kk.getName+","+kk.getSex+","+kk.getAge)
  }

  def test3(){
    val kryo = new Kryo()
    // Serialization of Scala enumerations
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(classOf[scala.Enumeration#Value])

    // Serialization of Scala maps like Trees, etc
    kryo.addDefaultSerializer(classOf[scala.collection.Map[_,_]], classOf[MapSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.generic.MapFactory[scala.collection.Map]], classOf[MapSerializer])

    // Serialization of Scala sets
    //kryo.addDefaultSerializer(classOf[scala.collection.Set[_]], classOf[SetSerializer])
//    kryo.addDefaultSerializer(classOf[scala.collection.generic.SetFactory[scala.collection.Set]], classOf[ScalaSetSerializer])

    // Serialization of all Traversable Scala collections like Lists, Vectors, etc
//    kryo.addDefaultSerializer(classOf[scala.collection.Traversable[_]], classOf[ScalaCollectionSerializer])
  }

}
