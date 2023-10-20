package com.atguigu.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author WEIYUNHUI
 * @date 2023/9/28 14:09
 *
 * 测试Jedis
 */
public class JedisDemo {

    public static void main(String[] args) {
        //Jedis jedis = getJedis();
        //Jedis jedis = getJedisFromPool();

        //使用
        //String result = jedis.ping();
        //System.out.println(result);


        //关闭
        //jedis.close();

//        testString();
        testZset();
    }

    /**
     * 作业: 测试每种类型的api方法， 每种类型至少5个方法。
     */
    public static void testString(){
        Jedis jedis = getJedisFromPool();

        // jedis中的api方法 与 shell命令一一对应。
        jedis.set("username" , "zhangsan") ;

        String email = jedis.get("email");
        System.out.println(email);

        jedis.close();
    }
    public static void testList(){
        Jedis jedis = getJedisFromPool();
        jedis.lpush("k1","v1");
        jedis.rpush("k1","v2","v3");
        System.out.println(jedis.llen("k1"));
        System.out.println(jedis.lrange("k1", 0, 2));
    }
    public static void testSet(){
        Jedis jedis = getJedisFromPool();
        jedis.sadd("k2","v1","v2","v3");
        System.out.println(jedis.sismember("k2", "v2"));
        System.out.println(jedis.scard("k2"));
        jedis.srem("k2","v1");
        System.out.println(jedis.smembers("k2"));
        System.out.println(jedis.scard("k2"));

    }
    public static void testZset(){
        Jedis jedis = getJedisFromPool();
        jedis.zadd("k3",555,"v1");
        jedis.zadd("k3",2333,"v2");
        jedis.zincrby("k3",5,"v2");
        System.out.println(jedis.zrangeByScore("k3", 1, 30000));
        System.out.println(jedis.zrank("k3","v2"));
        System.out.println(jedis.zcount("k3", 1, 100000));
        System.out.println(jedis.type("k3"));
        System.out.println(jedis.keys("*"));
        System.out.println(jedis.zrange("k3", 0, 10000));
        System.out.println(jedis.zrangeByScoreWithScores("k3", 1, 30000));
        jedis.del("k3");
    }
    public static void testHash(){
        Jedis jedis = getJedisFromPool();
        jedis.hset("k3","v1","a");
        jedis.hset("k3","v2","b");
        jedis.hset("k3","v3","cc");
        System.out.println(jedis.hkeys("k3"));
        System.out.println(jedis.hvals("k3"));
        jedis.del("k3");
    }




    public static String host = "hadoop102" ;
    public static int port = 6379 ;

    /**
     * 创建Jedis对象， 基于new的方式
     */
    public static Jedis getJedis(){
        //直接通过new的方式
        return new Jedis(host , port ) ;
    }

    /**
     * 连接池对象
     */
    public static JedisPool jedisPool ;


    /**
     * 创建Jedis对象 ， 基于连接池的方式
     */

    public static Jedis getJedisFromPool(){
        //判断池对象是否存在
        if(jedisPool == null ){
            //连接池配置
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            jedisPoolConfig.setMaxTotal(10); //最大可用连接数
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            //创建池对象
            jedisPool = new JedisPool(jedisPoolConfig ,  host , port) ;
        }

        //从连接池中获取一个Jedis对象
        Jedis jedis = jedisPool.getResource();
        return jedis ;
    }

}
