package cn.com.gxdgroup.dataplatform.demojava;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

/**
 * Created by bao on 14-6-13.
 */
public class SerialTest {
    private final static Jedis j = new Jedis("192.168.1.41", 6379);

    public static void main(String[] args) {

//        init();
//        test();

        kryoTest();

    }

    public static void kryoTest() {
        Kryo kryo = new Kryo();
        kryo.register(Person.class);

        Person p = new Person("wangwu", 20);

        Output out = new Output(1, 4096);
        kryo.writeObject(out, p);
        byte[] bytes = out.toBytes();

        j.set("wangwu".getBytes(), bytes);

        byte[] bs = j.get("wangwu".getBytes());
        Person b = (Person)kryo.readObject(new Input(bs), Person.class);
        System.out.println(b.name);
        System.out.println(b.age);

    }

    public static void test() {
        byte[] bytes = j.get("zhangsan".getBytes());
        Person a = (Person)SerializeUtil.unserialize(bytes);
        System.out.println(a.name);
        System.out.println(a.age);
        System.out.println("---------------------");

        bytes = j.get("lisi".getBytes());
        Person b = (Person)SerializeUtil.unserialize(bytes);
        System.out.println(b.name);
        System.out.println(b.age);
    }

    public static void init() {
        Person a = new Person("张三", 25);
        j.set("zhangsan".getBytes(), SerializeUtil.serialize(a));

        Person b = new Person("lisi", 22);
        j.set("lisi".getBytes(), SerializeUtil.serialize(b));
    }

}

class Person implements Serializable{
    public String name;
    public int age;
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Person(){}
}
