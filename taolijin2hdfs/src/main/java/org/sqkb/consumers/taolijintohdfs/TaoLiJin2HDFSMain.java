package org.sqkb.consumers.taolijintohdfs;

public class TaoLiJin2HDFSMain {
    public static void main(String[] args) {
        try {
            String propsName = "TLJKafka2HdfsApp.properties";
            ConsumerThread consumerThread = new ConsumerThread(propsName);
            consumerThread.start(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
