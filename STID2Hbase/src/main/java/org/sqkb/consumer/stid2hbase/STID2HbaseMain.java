package org.sqkb.consumer.stid2hbase;

import org.sqkb.utils.MyRedisUtil;

public class STID2HbaseMain {
    public static void main(String[] args) {
        try {
            MyRedisUtil.initJedis(10);
            String propsName = "STID2hbase.properties";
            new ConsumerThread(propsName, 10).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
