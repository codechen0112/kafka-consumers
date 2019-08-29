package org.sqkb.consumers.applogtohbase;


public class Applog2HbaseMain {
    public static void main(String[] args) {
        try {
            String propsName = "applog2hbase.properties";
            new ConsumerThread(propsName, 12).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
