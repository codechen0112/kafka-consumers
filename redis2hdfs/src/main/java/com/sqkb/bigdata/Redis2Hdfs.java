package com.sqkb.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;


public class Redis2Hdfs {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(Redis2Hdfs.class);
        try {


            FileInputStream fs = new FileInputStream(args[0]);


            FileReader fileReader = new FileReader(args[0]);
           String ymd = getYestoday();
            System.out.println(ymd);
           BufferedReader breader = new BufferedReader(fileReader);//
            Jedis jedis = getJedis();
            FSDataOutputStream stream = getHdfs(ymd);
            while (true) {

                String deviceId = breader.readLine();

                if (deviceId == null) {
                    break;
                }
                String key = "one_px" + deviceId;

                Set<String> set = jedis.smembers(key);

                if (!set.isEmpty()){
                    for (String shopId : set) {
                        String value = deviceId + "\t" + shopId+"\n";
                        stream.writeBytes(value);
                    }
                }
            }

            stream.close();
            jedis.close();
        } catch (Exception e) {
            log.info(e.toString());
            e.printStackTrace();
        }
    }

    private static String getYestoday() {
        Date today = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH,-1);
        Date yesToday = c.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return  format.format(yesToday);
    }

    private static Jedis getJedis() {
        String ip = "r-2zextlg3oemzd2trwo.redis.rds.aliyuncs.com";
        int port = 6379;
        String password = "FMQDDkkG4Btm8AuT";
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);  //最大连接数
        jedisPoolConfig.setMaxIdle(20);   //最大空闲
        jedisPoolConfig.setMinIdle(20);    //最小空闲
        jedisPoolConfig.setBlockWhenExhausted(true); //忙碌时是否等待
        jedisPoolConfig.setMaxWaitMillis(500);//忙碌时等待时长 毫秒
        jedisPoolConfig.setTestOnBorrow(true); //每次获得连接的进行测试
        JedisPool pool = new JedisPool(jedisPoolConfig, ip, port, 1000, password, 0, (String) null);
        return pool.getResource();
    }

    private static FSDataOutputStream getHdfs(String ymd) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(configuration);
        Path path = new Path("/user/hive/warehouse/hdw.db/deviceid_shopid/ymd=" + ymd
                + "/device_shopid" + "_" + ymd);
        if (!fs.exists(path)) {
            fs.createNewFile(path);
        }
        FSDataOutputStream stream = fs.append(path);
        return stream;
    }

}
