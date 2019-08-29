package org.sqkb.utils;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyRedisUtil {
    public static JedisPool pool = null;

    public static Jedis initJedis(int database) {
        if (pool == null) {
            String ip = "r-2ze990c3b92cf2a4.redis.rds.aliyuncs.com";
            int port = 6379;
            String password = "odoeZZn39BzG8AJ4";
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);  //最大连接数
            jedisPoolConfig.setMaxIdle(20);   //最大空闲
            jedisPoolConfig.setMinIdle(20);    //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true); //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(500);//忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true); //每次获得连接的进行测试
            pool = new JedisPool(jedisPoolConfig, ip, port, 1000, password, database, (String) null);
        }
        return pool.getResource();
    }

    public static Jedis getJedis() {
        return pool.getResource();
    }

    /**归还jedis对象*/
    public static void returnJedisOjbect(Jedis jedis){
        if (jedis != null) {
            jedis.close();
        }
    }



}



