package cn.com.gxdgroup.dataplatform.demo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by wq on 14-5-30.
 */


class WqRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //kryo.register(classOf[MyClass1])
    //kryo.register(classOf[MyClass2])
  }
}
