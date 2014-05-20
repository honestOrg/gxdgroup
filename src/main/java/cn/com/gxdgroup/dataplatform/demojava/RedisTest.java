package cn.com.gxdgroup.dataplatform.demojava;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.awt.*;

/**
 * Created by wq on 5/16/14.
 */
public class RedisTest {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        JedisPoolConfig config = config = new JedisPoolConfig();
        config.setMaxActive(60);
        config.setMaxIdle(1000);
        config.setMaxWait(10000);
        config.setTestOnBorrow(true);
        JedisPool pool = new JedisPool(config, "cloud41", 6379, 1000);

        Jedis j = pool.getResource();

        //Pipeline pipe = j.pipelined();

        System.out.println("connect time1:"+(System.currentTimeMillis()-start));

        long start1 = System.currentTimeMillis();

        for(int i=0;i<100000;i++){
            //pipe.hset("test33",Integer.toString(i),Integer.toString(i));
            j.hset("test33",Integer.toString(i),Integer.toString(i));
        }

        //pipe.sync();
        System.out.println("java time is:"+(System.currentTimeMillis()-start1)+" ms");
        pool.returnResource(j);
        pool.destroy();



    }
}
